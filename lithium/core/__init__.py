
#!/usr/bin/python
# -*- coding: utf-8 -*-

import copy

engine=None
_engine_name='sqlite3'

def get_engine():
    engine = __import__(_engine_name, globals(), locals(), ['object'], -1)
    return engine

def set_engine(engine_name):
    _engine_name=engine_name
    get_engine()

class QUERY(object):
    '''SQL builder to generate SQL statements'''
    __command = None
    __select = ''
    __update = ''
    __delete = ''
    __insert = ''
    __create = ''
    __columns = []
    __values = []
    values = []
    __from = ''
    __where = []
    __orderBy = ''
    __groupBy = ''
    __limit = ''

    def __init__(self):
        self.__command = None
        self.__select = ''
        self.__update = ''
        self.__delete = ''
        self.__insert = ''
        self.__create = ''
        self.__columns = []
        self.__values = []
        self.values = []
        self.__from = ''
        self.__where = []
        self.__orderBy = ''
        self.__groupBy = ''
        self.__limit = ''

    def CREATE(self, table):
        self.__command = 'create'
        self.__create = 'CREATE TABLE IF NOT EXISTS %s ' % table
        self.__create += '(%s);'
        return self


    def COLUMN(self, name, type):
        self.__columns.append('%s %s' % (name, type))
        return self


    def SELECT(self, *fields):
        self.__command = 'select'
        self.__select = 'SELECT '
        if(fields):
            self.__select += ', '.join(fields)
        else:
            self.__select += 'rowid, *'
        return self


    def UPDATE(self, table):
        self.__command = 'update'
        self.__update = 'UPDATE %s SET ' % table
        return self


    def SET(self, field, value):
        self.__values.append((field, value))
        return self


    def DELETE(self, table):
        self.__command = 'delete'
        self.__delete = 'DELETE FROM %s' % table
        return self


    def INSERT(self, table):
        self.__command = 'insert'
        self.__insert = 'INSERT INTO %s ' % table
        return self


    def VALUES(self, **values):
        self.__values = list({(k, values[k]) for k in values})
        return self


    def FROM(self, table):
        self.__from = ' FROM %s' % table
        return self


    def WHERE(self, field, operator, value, isRaw=False):
        self.__where.append((field, operator, value, isRaw))
        return self


    def WHERE_AND(self, field, operator, value, isRaw=False):
        if self.__where.__len__()>0:
            self.AND()
        self.__where.append((field, operator, value, isRaw))
        return self

    def AND(self):
        self.__where.append((None, 'AND', None, False))
        return self


    def OR(self):
        self.__where.append((None, 'OR', None, False))
        return self


    def LIMIT(self, offset, max):
        self.__limit = ' LIMIT %s,%s' % (offset, max)
        return self


    def GROUP_BY(self,*agrs, **operation_fields):
        fields=[]

        for operation in operation_fields.keys():
            if operation in ['SUM','COUNT','AVG']:
                for field in operation_fields[operation]:
                    fields.append(operation+'('+field+') as '+field)
            elif not operation in ['GROUP']:
                raise Exception('Operation '+operation+' for GROUP BY not supported')

        if (fields.__len__()>0):
            if self.__select != '':
                self.__select += ', '
            self.__select += ', '.join(fields)
            self.__groupBy = ' GROUP BY '
            self.__groupBy += ', '.join(operation_fields['GROUP'])

        return self

    def ORDER_BY(self, field, direction):
        self.__orderBy = ' ORDER BY %s %s' % (field, direction)
        return self


    def getValues(self):
        self.__repr__()
        return tuple(self.values) if self.values else ();


    def __repr__(self):
        sql = ''
        where = ''
        if(self.__where):
            where = ' WHERE '
            wherebuild = []
            for t in self.__where:
                if(not t[0] and not t[2]):
                    wherebuild.append(t[1])
                else:
                    wherebuild.append(('%s%s%s' % (t[0], t[1], t[2])) if t[3] else ('%s%s?' % (t[0], t[1])))
            where += ' '.join(wherebuild)

        if(self.__command == 'select'):
            sql = self.__select + self.__from + where + self.__groupBy + self.__orderBy + self.__limit + ';'
        elif (self.__command == 'insert'):
            sql = self.__insert + '(' + ','.join(['%s' % t[0] for t in self.__values]) + ') VALUES (' + ('?,'*len(self.__values))[:-1] + ');'
            self.values = [t[1] for t in self.__values]
        elif (self.__command == 'update'):
            sql = self.__update + ', '.join(['%s=%s' % (t[0], '?') for t in self.__values]) + where + ';'
            self.values = [t[1] for t in self.__values]
        elif (self.__command == 'delete'):
            sql = self.__delete + where + ';'
            self.values = [t[1] for t in self.__values]
        elif(self.__command == 'create'):
            sql = self.__create % ', '.join(self.__columns)

        if(self.__where):
            self.values = [t[2] for t in self.__where if (t[0] or t[2]) and not t[3]]

        return sql


class Database(object):
    '''Represents an easy to use interface to the database'''

    DatabaseError=get_engine().DatabaseError

    DB_FILE = None


    def __init__(self, dbfile=None, foreign_keys=False, parse_decltypes=False,*arg,**kargs):
        self.dbfile = dbfile if dbfile else Database.DB_FILE
        if (_engine_name.__eq__('sqlite3')):
            self.conn = get_engine().connect(self.dbfile, detect_types=(get_engine().PARSE_DECLTYPES if parse_decltypes else 0))
        elif _engine_name.__eq__('MySQLdb'): #using connection string for MySQLdb
            connectParams = dict(entry.split('=') for entry in self.dbfile.split(';'))
            self.conn = get_engine().connect(**connectParams)
        else:
            self.conn = get_engine().connect(self.dbfile,*arg,**kargs)

        self.db = self.conn.cursor()
        if(_engine_name.__eq__('sqlite3') and foreign_keys ):
            self.db.execute('PRAGMA foreign_keys = ON;')

    @classmethod
    def set_engine(self,engine_name):
        set_engine(engine_name)

    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    def close(self):
        if(self.conn):
            self.conn.close();


    def _get_model_column_names(self, model):
        return [n['name'] for n in model.columns()]


    def createTable(self, model):
        sql = QUERY().CREATE(model.tablename())
        for column in model.columns():
            sql.COLUMN(column['name'], column['type'])
        with Database() as db:
            db.db.execute(str(sql))
            db.conn.commit()


    def save(self, model):
        values = [model.__dict__[i] for i in self._get_model_column_names(model)]
        if(model.id):
            v = ','.join(['%s=?' % i for i in self._get_model_column_names(model)])
            sql = 'UPDATE %s SET %s WHERE rowid=?' % (model.tablename(), v)
            values += [model.id]
        else:
            f = ','.join(self._get_model_column_names(model))
            v = ('?,'*len(self._get_model_column_names(model)))[:-1]
            sql = 'INSERT INTO %s (%s) VALUES (%s)' % (model.tablename(), f, v)
        self.db.execute(sql, values)
        self.conn.commit()
        if(self.db.lastrowid):
            model.id = self.db.lastrowid
        return model.id


    def delete(self, model):
        if(model.id):
            sql = 'DELETE FROM %s WHERE rowid=?;' % model.tablename()
            self.db.execute(sql, (model.id,))
            self.conn.commit()
            return True
        else:
            return False


    def select(self, model, sql, values=()):
        if(sql.__class__ == QUERY):
            sql.SELECT().FROM(model.tablename())
            self.db.execute(str(sql), sql.getValues())
        else:
            self.db.execute(sql, values)
        columns = [t[0] for t in self.db.description]
        objects = []
        row = self.db.fetchone()
        mrange = None
        try:
            mrange = xrange
        except:
            mrange = range
        while(row):
            o = copy.deepcopy(model)
            for i in mrange(len(columns)):
                k = 'id' if columns[i] == 'rowid' else columns[i]
                o.__dict__[k]=row[i]
            row = self.db.fetchone()
            objects.append(o)
        return objects


    def selectOne(self, model, sql, values=()):
        res = self.select(model, sql, values)
        return res[0] if len(res) > 0 else None


    def selectById(self, model, id):
        return self.selectOne(model, QUERY().WHERE('rowid', '=', id))


    def getRaw(self, sql, values=(), max=-1):
        query = str(sql) if sql.__class__ == QUERY else sql
        self.db.execute(query, sql.values if sql.__class__ == QUERY else values)
        keys = [t[0] for t in self.db.description]
        return (keys, self.db.fetchmany(max))


    def getDict(self, sql, values=(), max=-1):
        header, raw = self.getRaw(sql, values, max)
        table = []
        for row in raw:
            obj = {}
            headIdx = 0
            for name in header:
                obj[name] = row[headIdx]
                headIdx += 1
            table.append(obj)
        return table


    def zeroZero(self, sql, values=()):
        query = str(sql) if sql.__class__ == QUERY else sql
        self.db.execute(query, sql.values if sql.__class__ == QUERY else values)
        row = self.db.fetchone()
        return row[0] if row else -1


    def table_exists(self, tablename):
        if (_engine_name.__eq__('sqlite3')):
            sql = "select * from sqlite_master where type='table' and name='%s';" % tablename
        elif (_engine_name.__eq__('MySQLdb')):
            sql = "SHOW TABLES LIKE '%s';" % tablename
        else:
            raise Exception('Not supported')
        return len(self.getDict(sql)) > 0


    def column_exists(self, tablename, columnname):
        if (_engine_name.__eq__('sqlite3')):
            sql = "pragma table_info('%s');" % tablename
        elif (_engine_name.__eq__('MySQLdb')):
            sql = "SHOW COLUMNS FROM `table` LIKE '%s';" % tablename
        else:
            raise Exception('Not supported')
        return len([r for r in self.getDict(sql) if r['name'] == columnname]) > 0



class Model(object):
    '''Abstracts the communication with the database and makes it easy to store objects'''
    id=0
    _dbfile=''
    foreign_keys=False
    parse_decltypes=False
    last_error=None

    def __init__(self, id=None, dbfile=None, foreign_keys=False, parse_decltypes=False):
        self.id = id
        self._dbfile = dbfile if dbfile is not None else Database.DB_FILE
        self.foreign_keys = foreign_keys
        self.parse_decltypes = parse_decltypes
        self.last_error = None


    def columns(self):
        pass


    def tablename(self):
        pass


    def __create_db(self):
        return Database(self._dbfile, foreign_keys=self.foreign_keys, parse_decltypes=self.parse_decltypes)


    def createTable(self):
        with self.__create_db() as db:
            db.createTable(self)


    def save(self):
        with self.__create_db() as db:
            db.save(self)


    def delete(self):
        with self.__create_db() as db:
            return db.delete(self)


    def getModel(self):
        if(self.id):
            with self.__create_db() as m:
                try:
                    model = m.selectById(self, self.id)
                    self.id = model.id
                    for name in m._get_model_column_names(model):
                        self.__dict__[name] = model.__dict__[name]
                except Exception as e:
                    self.last_error = e
                    self.id = None


    def select(self, sql):
        with self.__create_db() as m:
            return m.select(self, sql)


    def selectOne(self, sql):
        with self.__create_db() as m:
            return m.selectOne(self, sql)


    def selectCopy(self, sql):
        '''Run the SQL statement, select the first entry and copy
        the properties of the result object into the calling object.'''
        m = self.selectOne(sql)
        if(m):
            self.id = m.id
            for name in [n['name'] for n in m.columns()]:
                self.__dict__[name] = m.__dict__[name]
