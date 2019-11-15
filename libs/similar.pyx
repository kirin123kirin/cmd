
cimport cython
from _collections import _count_elements
from itertools import zip_longest
from functools import  lru_cache

cdef list BASE_TYPE = [type(None), int, float, str, bytes, bytearray, bool]

cpdef tuple flatten(object x):
    cdef list result = []
    try:
        for y in x:
            if type(y) in BASE_TYPE:
                result.append(y)
            else:
                result.extend(flatten(y))
        return tuple(result)
    except TypeError:
        return (x, )

cpdef tuple deephash(object x):
    try:
        return tuple([hash(y) if type(y) in BASE_TYPE else deephash(y) for y in x])
    except:
        return (hash(x), )

cdef inline object compare(object x, object y, object conditional_value, object delstr, object addstr):
    if x == y:
        return x
    elif x and y:
        return "{}{}{}".format(x, conditional_value, y)
    elif x:
        return "{}{}{}".format(x, conditional_value, delstr)
    else:
        return "{}{}{}".format(addstr, conditional_value, y)

cpdef object sanitize(object a, object b, object conditional_value=' ---> ', object delstr='DEL', object addstr='ADD'):
    if a is None or type(a) in BASE_TYPE and b is None or type(b) in BASE_TYPE:
        return compare(a, b, conditional_value, delstr, addstr)
    else:
        return [compare(x, y, conditional_value, delstr, addstr) for x, y in zip_longest(a, b, fillvalue="")]

cpdef double similar(tuple a, tuple b):
    """
        Parameters:
            a: tuple (Compare target data left)
            b: tuole (Compare target data right)
        Return:
            float (0.0 < return <= 1.000000000002)
    """
    cdef double prod = 0.0
    cdef int na, nb
    #cdef long long k #why slow?
    
    ca = {}
    cb = {}
    _count_elements(ca, a)
    _count_elements(cb, b)
    
    if ca and cb:
        in_b = cb.__contains__
        
        for k, na in ca.items():
            if in_b(k):
                nb = cb[k]
                if na <= nb:
                    prod += na
                else:
                    prod += nb
        
        if prod:
            return c_div(2*prod, c_add(len(a), len(b)))
    return 0.0

cdef inline double c_div(double a, double b):
    return a / b

cdef inline double c_add(double a, double b):
    return a + b

cdef inline double c_dot(double a, double b):
    return a * b

