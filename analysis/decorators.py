#!/usr/bin/env python

import time
import timeit

def timed(f):
    def dec(*args, **kwargs):
        start = timeit.default_timer()
        ret = f(*args, **kwargs)
        end = timeit.default_timer()
        print "The running time for %s : %2f" % (f.__name__, end - start)
        return ret

    return dec


if __name__ == "__main__":
    @timed
    def f(a, b):
        time.sleep(1)
        return a + b

    print f(1,3)
