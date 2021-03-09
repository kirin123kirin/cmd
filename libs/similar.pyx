# distutils: language=c++
# -*- coding: utf-8 -*-
cimport cython
from _collections import _count_elements
from itertools import zip_longest
from functools import  lru_cache
from libcpp.vector cimport vector

try:
    from Levenshtein._levenshtein import distance
except ModuleNotFoundError:
    distance = None

try:
    from editdistance import eval as onpdistance
except ModuleNotFoundError:
    onpdistance = None

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

cdef inline double c_div(double a, double b):
    return a / b

cdef inline double c_add(double a, double b):
    return a + b

cdef inline double c_minus(double a, double b):
    return a - b

cdef inline long minus(long a, long b):
    return a - b

cdef inline double c_dot(double a, double b):
    return a * b


cdef inline long snake(long k, long y, object left, object right):
    x = y - k
    while x < len(left) and y < len(right) and left[x] == right[y]:
        x += 1
        y += 1
    return y


@cython.boundscheck(False)
@cython.wraparound(False)
cpdef double similar(object left, object right):
    cdef long l1, l2
    cdef long sl1, sl2, offset, delta, p
    cdef double ed
    cdef vector[long] fp

    if left == right:
        return 1.0

    if not (left and right):
        return 0.0

    if distance and isinstance(left, str) and isinstance(right, str):
        return c_minus(1.0, c_div(distance(left, right), max(len(left), len(right))))

    try:
        l1, l2 = len(left), len(right)
        o1, o2 = left, right
    except:
        o1 = list(left)
        o2 = list(right)
        l1, l2 = len(o1), len(o2)

    if onpdistance:
        return 1.0 - (onpdistance(left, right) / max(l1, l2))
    else:
        s1 = o2 if l1 > l2 else o1
        s2 = o1 if l1 > l2 else o2

        sl1 = len(s1)
        sl2 = len(s2)

        offset = sl1 + 1
        delta = sl2 - sl1
        
        fp = [-1 for _ in range(sl1 + sl2 + 1)]

        p = 0
        while fp[delta + offset] != sl2:
            for k in range(-1 * p, delta):
                fp[k + offset] = snake(k, max(fp[k-1+offset] + 1, fp[k+1+offset]), s1, s2)
            for k in range(delta + p, delta, -1):
                fp[k + offset] = snake(k, max(fp[k-1+offset] + 1, fp[k+1+offset]), s1, s2)
            fp[delta + offset] = snake(delta, max(fp[delta-1+offset] + 1, fp[delta+1+offset]), s1, s2)

            p += 1

        ed = c_add(delta, c_minus(p, 1))

        return 1.0 - c_div(ed, max(l1,l2))

