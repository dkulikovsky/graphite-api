import requests
import itertools
import time
import string
import re
import ConfigParser

from hashlib import md5
from structlog import get_logger
from graphite_api.conductor import Conductor
from ..intervals import Interval, IntervalSet
from ..node import BranchNode, LeafNode


logger = get_logger()


class ClickHouseLeafNode(LeafNode):
    __fetch_multi__ = 'clickhouse'

class ClickHouseReader(object):
    __slots__ = ('path', 'config', 'schema', 'periods', 'storage', 'request_key')

    def __init__(self, path, config, request_key):
        self.config = config
        self.path = path
        self.request_key = request_key
        self.storage = config['clickhouse'].get('server', '127.0.0.1')
        return

    def fetch(self, start_time, end_time):
        start_t_g = time.time()
        logger.info("DEBUG:start_end_time:[%s] \t%s\t%s" % (self.request_key, start_time, end_time))
        params_hash = {}
        params_hash['query'], time_step = self.gen_query(start_time, end_time)
        logger.info("DEBUG:SINGLE:[%s] got query %s and time_step %d" % (self.request_key, params_hash['query'], time_step))
        url = "http://%s:8123" % self.storage
        dps = requests.get(url, params = params_hash).text
        if len(dps) == 0:
            logger.info("WARN: empty response from db, nothing to do here")
            return []

        # fill values array to fit (end_time - start_time)/time_step
        data = {}
        for dp in dps.split("\n"):
            dp = dp.strip()
            if len(dp) == 0:
                continue
            arr = dp.split("\t")
            dp_ts = arr[0].strip()
            dp_val = arr[1].strip()
            data[dp_ts] = float(dp_val)

        # fill output with nans when there is no datapoints
        filled_data = get_filled_data(data, start_time, end_time, time_step)

        # sort data
        sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
        time_info = start_time, end_time, time_step
        logger.info("RENDER:fetch:[%s] in %.3f" % (self.request_key, (time.time() - start_t_g)))
        return time_info, sorted_data

    def gen_query(self, stime, etime):
        coeff, agg = get_coeff(self.path, stime, etime)
        path_expr = "Path = '%s'" % self.path
        if agg == 0:
            query = """SELECT Time,Value FROM graphite_d WHERE %s\
                        AND Time > %d AND Time < %d AND Date >= toDate(toDateTime(%d)) AND 
                        Date <= toDate(toDateTime(%d))
                        ORDER BY Time, Timestamp""" % (path_expr, stime, etime, stime, etime) 
        else:
            sub_query = """SELECT Path,Time,Date,argMax(Value, Timestamp) as Value FROM graphite_d WHERE %s
                        AND Time > %d AND Time < %d AND Date >= toDate(toDateTime(%d)) AND 
                        Date <= toDate(toDateTime(%d)) GROUP BY Path, Time, Date""" % (path_expr, stime, etime, stime, etime) 
            query = """SELECT min(Time),avg(Value) FROM (%s) WHERE %s\
                        AND toInt32(kvantT) > %d AND toInt32(kvantT) < %d
                        AND Date >= toDate(toDateTime(%d)) AND Date <= toDate(toDateTime(%d))
                        GROUP BY Path, toDateTime(intDiv(toUInt32(Time),%d)*%d) as kvantT
                        ORDER BY kvantT""" % (sub_query, path_expr, stime, etime, stime, etime, coeff, coeff) 
        return query, coeff


    def get_intervals(self):
        start = 0
        end = max(start, time.time())
        return IntervalSet([Interval(start, end)])

class ClickHouseFinder(object):
    __fetch_multi__ = 'clickhouse'

    def __init__(self, config=None):
        self.config = config
        self.storage = config['clickhouse'].get('server', '127.0.0.1')
        # init storage schema 
        global schema
        global periods
        schema = {}
        periods = []
        schema_file = config['clickhouse'].get('schema', '/etc/cacher/storage_schema.ini')
        schema, periods = load_storage_schema(schema_file)


    def find_nodes(self, query):
        # get and store search query
        q = query.pattern
        self.search_query = q
        self.request_key = md5("%s:%f" % (q, time.time())).hexdigest()
        # search for metrics
        backend = self.config['clickhouse'].get('search', '127.0.0.1')
        metrics = mstree_search(q, backend)
        for path in metrics:
            # check if it is a branch or leaf
            if path[-1] == ".":
                yield BranchNode(path[:-1])
            else:
                yield ClickHouseLeafNode(path,ClickHouseReader(path, self.config, self.request_key))

    def fetch_multi(self, nodes, start_time, end_time):
        start_t_g = time.time()
        logger.info("DEBUG:start_end_time:[%s]\t%s\t%s" % (self.request_key, start_time, end_time))

        # fix path, convert it to query appliable to db
        paths = [node.path for node in nodes]
        data, time_step = self.get_multi_data(paths, start_time, end_time)
        # fullfill data fetched from storages to fit timestamps 
        result = {}
        start_t = time.time()
        time_info = start_time, end_time, time_step
        for path in data.keys():
            # fill output with nans when there is no datapoints
            filled_data = get_filled_data(data[path], start_time, end_time, time_step)
            sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
            result[path] = sorted_data
        logger.info("DEBUG:multi_fetch:[%s] all in in %.3f = [ fetch:%s, sort:%s ] path = %s" %\
		 (self.request_key, (time.time() - start_t_g), start_t - start_t_g, (time.time() - start_t), self.search_query))
        return time_info, result

    def get_multi_data(self, metrics, start_time, end_time):
        query, time_step, num = self.gen_multi_query(metrics, start_time, end_time)

        # query_hash now have only one storage beceause clickhouse has distributed table engine
        logger.info("DEBUG:MULTI:[%s] got storage %s, query [ %s ] and time_step %d" % (self.request_key, self.storage, query, time_step))
        start_t = time.time()

        url = "http://%s:8123" % self.storage
        data = {}
        dps = requests.post(url, query).text
        start_t = time.time()
        if len(dps) == 0:
            logger.info("WARN: empty response from db, nothing to do here")

        # fill values array to fit (end_time - start_time)/time_step
        for dp in dps.split("\n"):
            dp = dp.strip()
            if len(dp) == 0:
                continue
            arr = dp.split("\t")
            # and now we have 3 field insted of two, first field is path
            path = arr[0].strip()
            dp_ts = arr[1].strip()
            dp_val = arr[2].strip()
            data.setdefault(path, {})[dp_ts] = float(dp_val)
        fetch_time = time.time() - start_t
        logger.info("DEBUG:get_multi_data:[%s] fetch = %s, parse = %s, path = %s, num = %s" % (self.request_key, fetch_time, time.time() - start_t, query, num))
        return data, time_step


    def gen_multi_query(self, metrics, stime, etime):
        coeff, agg = get_coeff(self.search_query, stime, etime)
        query = ""
        num = len(metrics)
        metrics = [ "'%s'" % i for i in metrics ]
        path_expr = "Path IN ( %s )" % ", ".join(metrics)
        if agg == 0:
            query = """SELECT Path, Time, Value FROM graphite_d WHERE %s\
                        AND Time > %d AND Time < %d AND Date >= toDate(toDateTime(%d)) AND 
                        Date <= toDate(toDateTime(%d))
                        ORDER BY Time, Timestamp""" % (path_expr, stime, etime, stime, etime) 
        else:
            sub_query = """SELECT Path, Time, Date, argMax(Value, Timestamp) as Value
                        FROM graphite_d WHERE %s
                        AND Time > %d AND Time < %d 
                        AND Date >= toDate(toDateTime(%d)) AND Date <= toDate(toDateTime(%d))
                        GROUP BY Path, Time, Date""" % (path_expr, stime, etime, stime, etime) 
            query = """SELECT anyLast(Path), min(Time),avg(Value) FROM (%s)
                        WHERE %s\
                        AND toInt32(kvantT) > %d AND toInt32(kvantT) < %d 
                        AND Date >= toDate(toDateTime(%d)) AND Date <= toDate(toDateTime(%d))
                        GROUP BY Path, toDateTime(intDiv(toUInt32(Time),%d)*%d) as kvantT
                        ORDER BY kvantT""" % (sub_query, path_expr, stime, etime, stime, etime, coeff, coeff) 
        return query, coeff, num


# custom functions

def load_storage_schema(conf):
    schema_config = ConfigParser.ConfigParser()
    try:
        schema_config.read(conf)
    except Exception, e:
        logger.info("Failed to read conf file %s, reason %s" % (conf, e))
        return
    if not schema_config.sections():
        logger.info("absent or corrupted config file %s" % conf)
        return 
    
    schema = {}
    periods = []
    for section in schema_config.sections():
        if section == 'main':
            periods = [ int(x) for x in schema_config.get("main", "periods").split(",") ]
            continue
        if not schema.has_key(section):
            schema[section] = {}
            schema[section]['ret'] = []
            schema[section]['patt'] = schema_config.get(section, 'pattern')
        v = schema_config.get(section, "retentions")
        for item in v.split(","):
            schema[section]['ret'].append(int(item))
    return schema, periods

def get_coeff(path, stime, etime):
    # find metric type and set coeff
    coeff = 60
    agg = 0
    for t in schema.keys():
        if re.match(r'^%s.*$' % schema[t]['patt'], path):
            # calc retention interval for stime
            delta = 0
            loop_index = 0
            for seconds in schema[t]['ret']:
                # ugly month average 365/12 ~= 30.5
                # TODO: fix to real delta
                delta += int(periods[loop_index]) * 30.5 * 86400
                if stime > (time.time() - delta):
                    if loop_index == 0:
                        # if start_time is in first retention interval
                        # than no aggregation needed
                        agg = 0
                        coeff = int(seconds)
                    else:
                        agg = 1
                        coeff = int(seconds)
                    break
                loop_index += 1
            if agg == 0 and (stime < (time.time() - delta)):
                # start_time for requested period is even earlier than defined periods
                # take last retention
                seconds = schema[t]['ret'][-1]
                agg = 1
                coeff = int(seconds)
            break # break the outer loop, we have already found matching schema
    return coeff, agg

def get_filled_data(data, stime, etime, step):
    # some stat about how datapoint manage to fit timestamp map 
    ts_hit = 0
    ts_miss = 0
    ts_fail = 0
    start_t = time.time() # for debugging timeouts
    stime = stime - (stime % step)
    data_ts_min = int(min(data.keys()))
    data_stime = data_ts_min - (data_ts_min % step)
    filled_data = {}

    data_keys = sorted(data.keys())
    data_index = 0
    p_start_t_g = time.time()
    search_time = 0
    for ts in xrange(stime, etime, step):
        if ts < data_stime:
            # we have no data for this timestamp, nothing to do here
            filled_data[ts] = None
            ts_fail += 1
            continue

        ts = unicode(ts)
        if data.has_key(ts):
            filled_data[ts] = data[ts]
            data_index += 1
            ts_hit += 1
            continue
        else:
            p_start_t = time.time()
            for i in xrange(data_index, len(data_keys)):
                ts_tmp = int(data_keys[i])
                if ts_tmp >= int(ts) and (ts_tmp - int(ts)) < step:
                    filled_data[ts] = data[data_keys[data_index]]
                    data_index += 1
                    ts_miss += 1
                    break
                elif ts_tmp < int(ts):
                    data_index += 1
                    continue
                elif ts_tmp > int(ts):
                    ts_fail += 1
                    filled_data[ts] = None
                    break
            search_time += time.time() - p_start_t
        # loop didn't break on continue statements, set it default NaN value
        if not filled_data.has_key(ts):
            filled_data[ts] = None
    return filled_data


re_braces = re.compile(r'({[^{},]*,[^{}]*})')
re_conductor = re.compile(r'(^%[\w@-]+)$')
conductor = Conductor()

def mstree_search(q, backend):
    out = []
    for query in conductor_glob(q):
	    try:
	        res = requests.get("http://%s:7000/search?query=%s" % (backend, query))
	    except Exception, e:
	        return []
	    for item in res.text.split("\n"):
	        if not item: continue
	        out.append(item)
    return out


def braces_glob(s):
  match = re_braces.search(s)

  if not match:
    return [s]

  res = set()
  sub = match.group(1)
  open_pos, close_pos = match.span(1)

  for bit in sub.strip('{}').split(','):
    res.update(braces_glob(s[:open_pos] + bit + s[close_pos:]))

  return list(res)


def conductor_glob(pattern):
  parts = pattern.split('.')
  pos = 0
  found = False
  for part in parts:
    if re_conductor.match(part):
      found = True
      break
    pos += 1
  if not found:
    return braces_glob(pattern)
  cexpr = parts[pos]
  hosts = conductor.expandExpression(cexpr)

  if not hosts:
    return braces_glob(pattern)
  hosts = [host.replace('.','_') for host in hosts]

  braces_expr = '{' + ','.join(hosts) + '}'
  parts[pos] = braces_expr

  return braces_glob('.'.join(parts))



