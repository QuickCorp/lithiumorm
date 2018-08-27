#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 08/01/2018

@author: jeanmachuca

'''

__author__ = 'Jean Machuca'
__version__ = '0.0.1'
__license__ = 'LGPL'

import core
from .core import Database as DB
from .core import Model as COREModel
from .core import QUERY

DB.DB_FILE = 'sqlite3.db'

def set_db(dbname):
    core.DB_FILE = dbname

def set_engine(engine_name):
    core._engine_name=engine_name
    core.get_engine()

class Field(dict):
    pass

def IntegerProperty(name,*arg,**kargs):
    return Field(name=name,type='INTEGER')
def StringProperty(name,*arg,**kargs):
    return Field(name=name,type='TEXT')
def DateTimeProperty(name,*arg,**kargs):
    return Field(name=name,type='TEXT')
def FloatProperty(name,*arg,**kargs):
    return Field(name=name,type='TEXT')
def BooleanProperty(name,*arg,**kargs):
    return Field(name=name,type='TEXT')

_columns={}

class Model(COREModel):
    id = IntegerProperty('id')

    def __init__(self, *arg,**kargs):
        super(COREModel,self).__init__(*arg,**kargs)
        # Tries to fetch the object by its rowid from the database
        self.getModel()
        self.createTable()
    # Tells the database class the name of the database table
    def tablename(self):
        return self.__class__.__name__.lower()
    # Tells the database class more about the table columns in the database
    def columns(self):
        if not _columns.has_key(self.__class__.__name__.lower()):
            keys = filter(lambda k: (not type(vars(self.__class__)[k]).__name__.__eq__('function')) and (not k.startswith('__')),[v for v in vars(self.__class__)])
            _columns[self.__class__.__name__.lower()] = [vars(self.__class__)[key] for key in keys]
        return _columns[self.__class__.__name__.lower()]
    def put(self,*args,**kargs):
        self.save(*args,**kargs)


def Cube(query,filter_fields=None,group_fields=None,sum_fields=None,count_fields=None,avg_fields=None,extra_fields=None):
    extra_fields = extra_fields if not extra_fields is None else []
    sum_fields = sum_fields if not sum_fields is None else []
    count_fields = count_fields if not count_fields is None else []
    avg_fields = avg_fields if not avg_fields is None else []
    if not filter_fields is None:
        for k in filter_fields:
            query = query.WHERE_AND(k,'=',filter_fields[k])
#    if not extra_fields is None:
#        query = query.extra(select={e:e for e in extra_fields})

    query = query.SELECT() if group_fields is None else query.SELECT(*group_fields)
#    if not filter_fields is None:
#        query = query.extra(select=filter_fields)
    query = query.GROUP_BY(SUM=sum_fields,COUNT=count_fields,AVG=avg_fields,GROUP=group_fields)

    return query
