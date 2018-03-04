#!/usr/bin/env python
# *- encoding utf-8 -*
from functools import reduce

__all__ = ["flatten","walk_list","walk_tuple","walk_dict","walk_iteratable"]

def flatten(L):
    """ ex.
        >>> a = [1, [2, 3], (4, 5)]
        >>> print flatten(a)
        [1, 2, 3, 4, 5]
    """
    if isinstance(L, list) or isinstance(L, tuple):
        return reduce(lambda a,b: a + flatten(b), L, [])
    else:
        return [L]

def walk_list(f,L):
    if isinstance(L, list):
        return [walk_list(f,x) for x in L]
    else:
        return f(L)

def walk_tuple(f,T):
    if isinstance(T, tuple):
        return tuple(walk_tuple(f,x) for x in T)
    else:
        return f(T)

def walk_dict(f,D):
    if isinstance(D, dict):
        return {f(k):walk_dict(f,v) for (k,v) in list(D.items())}
    else:
        return f(D)

def walk_iteratable(f, M):
    if M is None:
        return M
    elif isinstance(M, list):
        return [walk_iteratable(f,x) for x in M]
    elif isinstance(M, tuple):
        return tuple(walk_iteratable(f,x) for x in M)
    elif isinstance(M, dict):
        return {f(k):walk_iteratable(f,v) for (k,v) in list(M.items())}
    else:
        return f(M)

if __name__ == '__main__':
    li = [2, [42, 44], 6, [82, [842, 844], 86, 88], 10]
    tu = (2, (42, 44), 6, (82, (842, 844), 86, 88), 10)
    di = {2:{42:44},6:{82:{842:88},88:77,10:1}}
    mix = [2, (42, 44), [6, {6:{82:{842:88}}}, (86, 88)], 10]
    assert walk_list(str,li)
    assert walk_tuple(str,tu)
    assert walk_dict(str,di)
    assert walk_iteratable(str, mix)