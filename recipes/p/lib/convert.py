#coding:utf-8



class Convert(object):

    @classmethod

    def bs2utf8(cls, basestr):

        if not basestr or not isinstance(basestr, basestring):

            return basestr

            

        return basestr.encode('utf-8') if isinstance(basestr, unicode) else basestr

        

        

    @classmethod

    def mongo2utf8(cls, d):

        if isinstance(d, unicode):

            return cls.bs2utf8(d)

        elif isinstance(d, (list, tuple)):

            return [cls.mongo2utf8(x) for x in d]

        elif isinstance(d, dict):

            ret = {}

            for k, v in d.items():

                if isinstance(k, unicode):

                    k = cls.bs2utf8(k)

                if isinstance(v, unicode):

                    v = cls.bs2utf8(v)

                elif isinstance(v, (list, tuple)):

                    v = [cls.mongo2utf8(x) for x in v]

                elif isinstance(v, dict):

                    v = cls.mongo2utf8(v)



                ret[k] = v

            return ret

        else:

            return d

            

                      
