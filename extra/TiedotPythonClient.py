# -*- coding: utf-8 -*-
"""
Python module to connect to golang tiedot database
https://github.com/HouzuoGuo/tiedot
http://golang.org/

Depends on python request library: http://requests.readthedocs.org/en/latest/

Courtesy of Mr.Cristian Echeverria - thank you!
"""

import json
import requests

#-----------------------------------------------------------------------------------------

__all__ = ["DbException", "Connection"]

#-----------------------------------------------------------------------------------------

class DbException(Exception):
    pass

#-----------------------------------------------------------------------------------------

class _TiedotConnection(object):
    
    def __init__(self, host="127.0.0.1", port=8080):
        self.host = host
        self.port = port
    
    def _get(self, cmd, normal_code=200, **params):
        url = "http://%s:%d/%s" % (self.host, self.port, cmd)
        r = requests.get(url, params=params)
        if r.status_code != normal_code:
            raise DbException(r.text)
        return r

#-----------------------------------------------------------------------------------------

class Connection(_TiedotConnection):
    
    def create(self, col, numparts=2):
        self._get("create", 201, col=col, numparts=numparts)
        return Collection(col, self.host, self.port)
    
    def all(self):
        r = self._get("all")
        return r.json()
    
    def strcol(self):
        return [x for x in self.all().iterkeys()]
    
    def use(self, col):
        if col in self.strcol():
            return Collection(col, self.host, self.port)
        else:
            raise DbException("Collection %s does not exist" % col)
    
    def dump(self, dest):
        self._get("dump", dest=dest)
    
    def shutdown(self):
        try:
            self._get("shutdown")
        except:
            return
    
    def memstats(self):
        r = self._get("memstats")
        return r.json()
    
    def version(self):
        r = self._get("version")
        return r.text
    
    # Following methods can be used from Collection class
    
    def rename(self, old, new):
        self._get("rename", old=old, new=new)
    
    def drop(self, col):
        self._get("drop", col=col)
    
    def scrub(self, col):
        self._get("scrub", col=col)

    def repartition(self, col, numparts):
        self._get("repartition", col=col, numparts=numparts)

    def insert(self, col, doc):
        r = self._get("insert", 201, col=col, doc=json.dumps(doc))
        return r.text
    
    def get(self, col, id):
        r = self._get("get", col=col, id=id)
        return r.json()
    
    def update(self, col, id, doc):
        self._get("update", col=col, id=id, doc=json.dumps(doc))
    
    def delete(self, col, id):
        self._get("delete", col=col, id=id)
    
    def flush(self):
        self._get("flush")
    
    def index(self, col, path):
        self._get("index", 201, col=col, path=path)
    
    def unindex(self, col, path):
        self._get("unindex", col=col, path=path)
    
    def indexes(self, col):
        r = self._get("indexes", col=col)
        return r.json()
    
    def query(self, col, q):
        r = self._get("query", col=col, q=json.dumps(q))
        return r.json()

    def count(self, col, q):
        r = self._get("count", col=col, q=json.dumps(q))
        return r.json()

#-----------------------------------------------------------------------------------------

class Collection(_TiedotConnection):

    def __init__(self, name, host="127.0.0.1", port=8080):
        super(Collection, self).__init__(host, port)
        self.name = name

    def rename(self, new):
        self._get("rename", old=self.name, new=new)
        self.name = new
    
    def drop(self):
        self._get("drop", col=self.name)
    
    def scrub(self):
        self._get("scrub", col=self.name)

    def repartition(self, numparts):
        self._get("repartition", col=self.name, numparts=numparts)
    
    def insert(self, doc):
        r = self._get("insert", 201, col=self.name, doc=json.dumps(doc))
        return r.text
    
    def get(self, id):
        r = self._get("get", col=self.name, id=id)
        return r.json()
    
    def update(self, id, doc):
        self._get("update", col=self.name, id=id, doc=json.dumps(doc))
    
    def delete(self, id):
        self._get("delete", col=self.name, id=id)
    
    def index(self, path):
        self._get("index", 201, col=self.name, path=path)
    
    def unindex(self, path):
        self._get("unindex", col=self.name, path=path)
    
    def indexes(self):
        r = self._get("indexes", col=self.name)
        return r.json()
    
    def query(self, q):
        r = self._get("query", col=self.name, q=json.dumps(q))
        return r.json()
    
    def count(self, q):
        r = self._get("count", col=self.name, q=json.dumps(q))
        return r.json()
