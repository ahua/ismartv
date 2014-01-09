#coding:utf-8



from datetime import datetime

from convert import Convert

from itertools import imap





class SysLog(object):

    def __init__(self, ts, sn):

        assert ts and isinstance(ts, datetime)

        assert sn and isinstance(sn, basestring)

        

        self.__ts = ts

        self.__sn = sn

        self.__fields = []

        self.__kset = {}

        

        

    def copy_into(self, ignore_keys, kw):

        assert isinstance(ignore_keys, (list, tuple))

        assert kw and isinstance(kw, dict)

        

        for k, v in kw.items():

            if k in ignore_keys:

                continue

            self.append_field(k, v)

        

        

    def append_field(self, k, v):

        assert k and isinstance(k, basestring)

        if k in self.__kset:

            return

            

        k = Convert.bs2utf8(k)

        v = Convert.bs2utf8(v)

            

        self.__fields.append('{0}={1}'.format(k, v))

        self.__kset.update({k: v})

        

        

    def get(self, f):

        if 'ts' == f:

            return self.__ts

        if 'sn' == f:

            return self.__sn

        assert f in self.__kset, f

        return self.__kset.get(f)

        

        

    def dumps(self):

        return '\t'.join(self.__iter__())

        

        

    def __iter__(self):

        yield self.__ts.strftime('%Y-%m-%d %H:%M:%S')

        yield self.__sn

        for i in self.__fields:

            yield i

        

        

    @staticmethod

    def loads(line, filter_func):

        try:

            recs = line.strip().split('\t')

        

            ts = datetime.strptime(recs[0], '%Y-%m-%d %H:%M:%S')

            sn = recs[1]

        

            props = dict(imap(lambda x: x.split('=', 1), recs[2:]))

            props.update({'ts': ts})

            props.update({'sn': sn})

            

            if not filter_func:

                return False, props

                

            for f, funcs in filter_func.items():

                value = props.get(f)

                if not value:

                    continue

                if any(imap(lambda x: x(value), funcs)):

                    return True, props



            return False, props

        except ValueError as e:

            return True, None     

            

               
