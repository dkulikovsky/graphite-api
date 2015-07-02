import requests
import itertools
import time
import string
import re
import ConfigParser
from pprint import pprint
from graphite.logger import log
from collections import OrderedDict
from django.conf import settings
from graphite.storage import FindQuery
from graphite.conductor import Conductor

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

class tmp_node_obj():
    def __init__(self, path):
        self.path = path


class ClickHouseLeafNode(LeafNode):
    __fetch_multi__ = 'clickhouse'

class ClickHouseReader(object):
    __slots__ = ('path','schema', 'periods', 'multi', 'pathExpr', 'storage', 'request_key')

    def __init__(self, path, storage="", multi=0, reqkey=""):
        self.storage = storage
        if multi:
            self.multi = 1
            self.pathExpr =  path
        else:
            self.multi = 0
            self.path = path
        self.schema = {}
        self.periods = []
        self.load_storage_schema()
        self.request_key = reqkey
        try:
            self.storage = "".join(getattr(settings, 'CLICKHOUSE_SERVER'))
        except:
            self.storage = "127.0.0.1"

    def load_storage_schema(self):
        conf = "/etc/cacher/storage_schema.ini"
        config = ConfigParser.ConfigParser()
        try:
            config.read(conf)
        except Exception, e:
            log.info("Failed to read conf file %s, reason %s" % (conf, e))
            return
        if not config.sections():
            log.info("absent or corrupted config file %s" % conf)
            return 
        
        schema = {}
        periods = []
        for section in config.sections():
            if section == 'main':
                periods = [ int(x) for x in config.get("main", "periods").split(",") ]
                continue
            if not schema.has_key(section):
                schema[section] = {}
                schema[section]['ret'] = []
                schema[section]['patt'] = config.get(section, 'pattern')
            v = config.get(section, "retentions")
            for item in v.split(","):
                schema[section]['ret'].append(int(item))
        self.schema = schema
        self.periods = periods
        return 

    def fetch_multi(self, nodes, start_time, end_time):
        start_t_g = time.time()
        log.info("DEBUG:start_end_time:[%s]\t%s\t%s" % (self.request_key, start_time, end_time))

        # fix path, convert it to query appliable to db
        paths = [node.path for node in nodes]
        data, time_step = self.get_multi_data(paths, start_time, end_time)
        # fullfill data fetched from storages to fit timestamps 
        result = {}
        start_t = time.time()
        time_info = start_time, end_time, time_step
        for path in data.keys():
            # fill output with nans when there is no datapoints
            filled_data = self.get_filled_data(data[path], start_time, end_time, time_step)
            sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
            result[path] = sorted_data
        log.info("DEBUG:multi_fetch:[%s] all in in %.3f = [ fetch:%s, sort:%s ] path = %s" %\
		 (self.request_key, (time.time() - start_t_g), start_t - start_t_g, (time.time() - start_t), self.pathExpr))
        return time_info, result

    def get_multi_data(self, metrics, start_time, end_time):
        query, time_step, num = self.gen_multi_query(metrics, start_time, end_time)

        # query_hash now have only one storage beceause clickhouse has distributed table engine
        log.info("DEBUG:MULTI:[%s] got storage %s, query [ %s ] and time_step %d" % (self.request_key, self.storage, query, time_step))
        start_t = time.time()

        url = "http://%s:8123" % self.storage
        data = {}
        dps = requests.post(url, query).text
        start_t = time.time()
        if len(dps) == 0:
            log.info("WARN: empty response from db, nothing to do here")

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
        log.info("DEBUG:get_multi_data:[%s] fetch = %s, parse = %s, path = %s, num = %s" % (self.request_key, fetch_time, time.time() - start_t, self.path, num))
        return data, time_step


    def gen_multi_query(self, metrics, stime, etime):
        coeff, agg = self.get_coeff(stime, etime)
        query = ""
        num = len(metrics)
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


    def fetch(self, start_time, end_time):
        start_t_g = time.time()
        log.info("DEBUG:start_end_time:[%s] \t%s\t%s" % (self.request_key, start_time, end_time))
        params_hash = {}
        params_hash['query'], time_step = self.gen_query(start_time, end_time)
        log.info("DEBUG:SINGLE:[%s] got query %s and time_step %d" % (self.request_key, params_hash['query'], time_step))
        url = "http://%s:8123" % self.storage
        dps = requests.get(url, params = params_hash).text
        if len(dps) == 0:
            log.info("WARN: empty response from db, nothing to do here")
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
        filled_data = self.get_filled_data(data, start_time, end_time, time_step)

        # sort data
        sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
        time_info = start_time, end_time, time_step
        log.info("RENDER:fetch:[%s] in %.3f" % (self.request_key, (time.time() - start_t_g)))
        return time_info, sorted_data

    def get_coeff(self, stime, etime):
        # find metric type and set coeff
        coeff = 60
        agg = 0
        for t in self.schema.keys():
            if re.match(r'^%s.*$' % self.schema[t]['patt'], self.path):
                # calc retention interval for stime
                delta = 0
                loop_index = 0
                for seconds in self.schema[t]['ret']:
                    # ugly month average 365/12 ~= 30.5
                    # TODO: fix to real delta
                    delta += int(self.periods[loop_index]) * 30.5 * 86400
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
                    seconds = self.schema[t]['ret'][-1]
                    agg = 1
                    coeff = int(seconds)
                break # break the outer loop, we have already found matching schema
        return coeff, agg

    def gen_query(self, stime, etime):
        coeff, agg = self.get_coeff(stime, etime)
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

    def get_filled_data(self, data, stime, etime, step):
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

    def get_intervals(self):
        start = 0
        end = max(start, time.time())
        return IntervalSet([Interval(start, end)])

class ClickHouseFinder(object):
    __fetch_multi__ = 'clickhouse'

    def find_nodes(self, query, request_key):
        q = query.pattern
        metrics = mstree_search(q)
        for v in metrics:
            if v[-1] == ".":
                yield BranchNode(v[:-1])
            else:
                yield ClickHouseLeafNode(v,ClickHouseReader(v, reqkey=request_key))


# custom search functions

re_braces = re.compile(r'({[^{},]*,[^{}]*})')
re_conductor = re.compile(r'(^%[\w@-]+)$')
conductor = Conductor()

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


def mstree_search(q):
    out = []
    for query in conductor_glob(q):
            try:
                backend = getattr(settings, 'METRICSEARCH')
            except:
                backend = "127.0.0.1"
	    try:
	        res = requests.get("http://%s:7000/search?query=%s" % ("".join(backend), query))
	    except Exception, e:
	        return []
	    for item in res.text.split("\n"):
	        if not item: continue
	        out.append(item)
#    log.info("DEBUG:mstree_search: got %d items from search" % len(out))
    return out
