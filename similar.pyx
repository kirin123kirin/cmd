#from functools import _CacheInfo, _lru_cache_wrapper

#cdef double cy_mssqrt(tuple c):
#    cdef int ret = 0
#    for x in c:
#        ret += x * x
#    return ret ** 0.5
#
#mssqrt =_lru_cache_wrapper(cy_mssqrt, 128, False, _CacheInfo)

from collections import _count_elements as Counter

def similar(tuple a, tuple b):
    """
        Parameters:
            a: tuple (Compare target data left)
            b: tuole (Compare target data right)
        Return:
            float (0.0 < return <= 1.000000000002)
    """

    cdef dict ca = {}
    Counter(ca, a)
    cdef dict cb = {}
    cdef dict cab = {}
    cdef int prod = 0

    for k in b:
        if k in cb:
            cb[k] += 1
        else:
            cb[k] = 1

        if k in ca:
            cab[k] = ca[k] + cb[k]

    if cab:
        for v in cab.values():
            prod += v
        return prod / (len(a) + len(b))
    return 0.0
