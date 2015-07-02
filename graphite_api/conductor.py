#!/usr/bin/env python
import httplib2, json, time, re, memcache

class Conductor():
  
  CONDUCTOR_HOST = "c.yandex-team.ru"
  API_URL = "/api-cached"
  EXPIRES_IN = 600
  CONDUCTOR_EXPR_RE = re.compile(r'^%([\w-]+)(@(\w+))?')

  def __init__(self, host=CONDUCTOR_HOST):
    self.host = host
    self.__http = httplib2.Http()
    self.url = "http://" + self.host + Conductor.API_URL
    self.mc = memcache.Client(['127.0.0.1:11211'])

  def expandExpression(self, expr, cached=True):
    result = self.mc.get(expr.encode('utf-8'))
    if cached and result:
      return result
    else:
      try: 
        return self.__expandExpressionNonCached(expr)
      except:
        return []
      
  def __expandExpressionNonCached(self, expr):
    match = Conductor.CONDUCTOR_EXPR_RE.search(expr)
    if not match:
      raise ValueError('Not supported conductor expression: %s' % expr)
    (group,_,dc) = match.groups()
    response, content = self.__http.request(self.url + "/groups2hosts/" + group + "?format=json&fields=fqdn,datacenter_name,root_datacenter_name", "GET")
    result = []
    if response['status'] == '200':
      data = json.loads(content)
      for host in data:
        if dc and not dc in (host['datacenter_name'], host['root_datacenter_name']): continue
        result.append(host['fqdn'])
      result.sort()
      if result:
        self.mc.set(expr.encode('utf-8'),result,time=Conductor.EXPIRES_IN)
    return result

  def expandExpressionFull(self, expr, cached=True):
    result = self.mc.get("%s_full" % expr.encode('utf-8'))
    if cached and result:
      return result
    else:
      try: 
        return self.__expandExpressionFullNonCached(expr)
      except:
        return []
      
  def __expandExpressionFullNonCached(self, expr):
    match = Conductor.CONDUCTOR_EXPR_RE.search(expr)
    if not match:
      raise ValueError('Not supported conductor expression: %s' % expr)
    (group,_,dc) = match.groups()
    response, content = self.__http.request(self.url + "/groups2hosts/" + group + "?format=json&fields=fqdn,datacenter_name,root_datacenter_name", "GET")
    result = []
    if response['status'] == '200':
      data = json.loads(content)
      for host in data:
        if dc and not dc in (host['datacenter_name'], host['root_datacenter_name']): continue
        result.append(host)
      if result:
        self.mc.set("%s_full" % expr.encode('utf-8'),result,time=Conductor.EXPIRES_IN)
    return result
