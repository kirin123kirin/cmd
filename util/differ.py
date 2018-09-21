#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""
__version__ = "0.2.0"
__author__ = "m.yama"

from util.core import (
        listlike,
        is1darray,
        isposkey,
        is2darray,
        isdataframe,
        sortedrows,
        iterrows,
        flatten,
        sorter,
        kwtolist,
        values_not,
        )

from util.profiler import Profile

import os
import sys
from itertools import chain, zip_longest
from difflib import SequenceMatcher
from collections import namedtuple


__all__ = ["differ"]


def sanitize(a, b):
    def comp(x, y):
        if x == y:
            return x
        elif x and not y:
            return "{} ---> DEL".format(x)
        elif not x and y:
            return "ADD ---> {}".format(y)
        else:
            return "{} ---> {}".format(x, y)
    if isinstance(a, list) and isinstance(b, list):
        return [comp(*x) for x in zip_longest(a, b, fillvalue="")]
    else:
        return comp(a, b)

dinfo = namedtuple("DiffInfo", ("tag", "indexa", "indexb", "value"))

def iterdiff1D(A, B, skipequal=True, na_value=""):
    """ 1D data list diffs function
    Parameter:
        A        : iterable 2D data (need sorted)
        B        : iterable 2D data (need sorted)
        skipequal: True is equal non output
        na_value : line number output value if delete or insert
    Returns:
        tuple (tags, rownum_of_A, rownum_of_B, value)
    """
    _A, _B = listlike(A), listlike(B)
    seq = SequenceMatcher(None, _A, _B , autojunk=False)
    nul = [None, None]
    for group in seq.get_grouped_opcodes():
        for tag, i1, i2, j1, j2 in group:
            lon = zip_longest(_A[i1:i2], _B[j1:j2], fillvalue=nul)
            for (i, a), (j, b) in lon:
                if a == b:
                    if skipequal:
                        continue
                    yield dinfo("equal", i, j, a)
                elif not a:
                    yield dinfo("insert", na_value, j, b)
                elif not b:
                    yield dinfo("delete", i, na_value, a)
                else:
                    yield dinfo("replace", i, j, sanitize(a, b))


def iterdiff2D(A, B, compare, skipequal=True, na_value=""):
    """ 2D data list diffs function
    Parameter:
        A        : iterable 2D data (need sorted)
        B        : iterable 2D data (need sorted)
        compare  : takes an A item and a B item and returns <0, 0 or >0
        skipequal: True is equal non output
        na_value : line number output value if delete or insert
    Returns:
        tuple (tags, rownum_of_A, rownum_of_B, diff_result_columns )
    """
    ia, ib = True, True
    isfirst = True
    while True:
        if ia:
            na, a = next(A, [-1,None])
        if ib:
            nb, b = next(B, [-1,None])

        if b is None:
            if a is None:
                break
            yield dinfo("delete", na, na_value, a)
            ia, ib = True, False
        elif a is None:
            yield ("insert", na_value, nb, b)
            ia, ib = False, True
        elif a == b:
            if isfirst or skipequal is False:
                yield dinfo("equal", na, nb, a)
            ia, ib = True, True
        elif compare(a, b) < 0:
            yield dinfo("delete", na, na_value, a)
            ia, ib = True, False
        elif compare(a, b) > 0:
            yield dinfo("insert", na_value, nb, b)
            ia, ib = False, True
        else:
            assert compare(a, b) == 0
            yield dinfo("replace", na, nb, sanitize(a, b))
            ia, ib = True, True
        if isfirst:
            isfirst = False

def compare_build(A, B, keya, keyb, startidx=1):
    def getfunc(o, key):
        if is1darray(o):
            return str
        elif callable(key):
            return key
        elif isposkey(key):
            return lambda x: [x[i] for i in key]
        elif is2darray(o):
            if startidx == "infer":
                r = [o[0].index(k) - 1 for k in key]
            else:
                r = [o[0].index(k) for k in key]
            return lambda x: [x[i] for i in r]
        elif isdataframe(o):
            return lambda x: [x[i] for i in [o.columns.tolist().index(k) for k in key]]
        else:
            raise ValueError("Unknown values o is `{}`  key is `{}`".format(type(o), key))

    # compare function initialize
    func_a = getfunc(A, keya)
    func_b = getfunc(B, keyb)

    def compare(a,b):
        if func_a(a) < func_b(b):
            return -1
        elif func_a(a) > func_b(b):
            return 1
        else:
            return 0

    return compare

def helperdiff1D(A, B, sort=True, skipequal=True, startidx=1):
    func = sortedrows if sort else iterrows
    # header out
    yield ["DIFFTAG","LEFTLINE#", "RIGHTLINE#", "VALUE"]
    for r in iterdiff1D(func(A, start=startidx),
                     func(B, start=startidx),
                     skipequal=skipequal):
        yield list(r)

def helperdiff2D(A, B, keya=[], keyb=[], sort=True, skipequal=True, startidx=1):

    # parameter initialize
    try:
        keya = keya or range(len(isdataframe(A) and A.columns.tolist() or A[0]))
    except:
        pass

    keyb = keyb or keya

    compare = compare_build(A, B, keya, keyb, startidx)
    # compute diff run
    if sort is False:
        ret = iterdiff2D(iterrows(A, startidx),
                         iterrows(B, startidx),
                         compare = compare,
                         skipequal=skipequal)
    else:
        ret = iterdiff2D(sortedrows(A, keya, startidx),
                         sortedrows(B, keyb, startidx),
                         compare = compare,
                         skipequal=skipequal)

    # header out
    yield flatten(["DIFFTAG","LEFTLINE#", "RIGHTLINE#", list(next(ret)[3:])])

    for r in ret:
        yield flatten(r)

def diffauto(a, b, skipequal=True, startidx=1):
    cola = a.columns.tolist()
    colb = b.columns.tolist()
    dka = {tuple(cola.index(z) for z in v):k for k, v in Profile(a.head(10), top=None).diffkey.items()}
    dkb = {tuple(colb.index(z) for z in v) for v in Profile(b.head(10), top=None).diffkey.values()}
    diffkeys=sorted((dka[x], x) for x in set(dka) & dkb)

    for i, (_, key) in enumerate([(None, None)] + diffkeys):
        if i == 0:
            r = helperdiff2D(a, b, skipequal=skipequal, startidx=startidx)
            header = next(r)
            yield header
            _h = ["line"] + header[3:]
            reta, retb = [_h], [_h]
        else:
            r = helperdiff2D(reta.copy(), retb.copy(), keya=key, keyb=key, skipequal=skipequal, startidx="infer")
            reta, retb = [_h], [_h]
        for x in r:
            if x[0] in ["equal", "replace"]:
                yield x
            elif x[0] == "delete":
                reta.append([x[1], *x[3:]])
            elif x[0] == "insert":
                retb.append([x[2], *x[3:]])
    else:
        #Last Loop
        for y in reta[1:]:
            yield ["delete", y[0], "", *y[1:]]
        for y in retb[1:]:
            yield ["insert", "", y[0], *y[1:]]

def dictdiffer(a, b, keya=[], keyb=[], sort=True, skipequal=True, startidx=1):
    r = helperdiff1D(list(a), list(b), sort=False, skipequal=False, startidx=startidx)
    next(r)
    i = 0

    for tag, ll, rl, val in r:
        if tag == "equal":
            _a, _b = a[val], b[val]
        elif tag == "replace":
            va, vb = val.split(" ---> ")
            _a, _b = a[va], b[vb]
        elif tag == "insert":
            _a, _b = [], b[val]
        elif tag == "delete":
            _a, _b = a[val], []

        rr = differ(_a, _b, keya=keya, keyb=keyb, sort=sort, skipequal=skipequal, startidx=startidx)

        header = next(rr)
        header.insert(3, "TARGET")
        if i == 0:
            yield header
        for d in rr:
            d.insert(3, val)
            yield d
            i += 1

def differ(A, B, keya=[], keyb=[], sort=True, skipequal=True, startidx=1):
    """
    """
    # inital check
    try:
        if isdataframe(A) and isdataframe(B) and A.columns.tolist() == B.columns.tolist() and len(A) == len(B):
            ck = (A == B).all().all()
            if hasattr(ck, "compute"):
                ck = ck.compute()
            if ck and not skipequal:
                header = [["DIFFTAG","LEFTLINE#", "RIGHTLINE#"]] + [A.columns.tolist()]
                return chain(header, (["equal", i, i] + list(x) for i, x in enumerate(A.itertuples(),startidx+1)))
        else:
            ck = (A is B) or (A == B)
            if ck:
                if not skipequal and is1darray(A):
                    return (["equal", i, i] + x for i, x in enumerate(A))
                elif not skipequal and is2darray(A):
                    header = [["DIFFTAG","LEFTLINE#", "RIGHTLINE#"] + A[0]]
                    return chain(header, (["equal", i, i] + list(x) for i, x in enumerate(A)))
        if ck and skipequal:
            return []
    except:
        pass

    if is1darray(A) and is1darray(B):
        ret = helperdiff1D(A, B, sort=sort, skipequal=skipequal, startidx=startidx)
    elif isinstance(A, dict) and isinstance(B, dict):
        ret = dictdiffer(A, B, keya=keya, keyb=keyb, skipequal=skipequal, startidx=startidx)
    elif not keya and not keyb:
        ret = diffauto(A, B, skipequal=skipequal, startidx=startidx)
    else:
        ret = helperdiff2D(A, B, keya=keya, keyb=keyb, sort=sort, skipequal=skipequal, startidx=startidx)

    # header out
    header = [next(ret)]
    content = (flatten(r) for r in sorter(ret, key=lambda x: (x[2] or x[1] or -1)))
    # output sorted
    return chain(header, content)



def main():
    from util.dfutil import read_any
    from argparse import ArgumentParser
    import codecs
    import csv

    ps = ArgumentParser(prog="differ",
                        description="2 file diff compare program\n")
    padd = ps.add_argument

    padd('-V','--version', action='version', version='%(prog)s ' + __version__)
    padd('file1', nargs=1, help='diff before file')
    padd('file2', nargs=1, help='diff after file')

    padd('-v', '--verbose', action='store_true', default=False,
         help='Progress verbose output.')
    padd('-a', '--all', action='store_true', default=False,
         help='All Line print(default False)')
    padd('-o', '--outfile', type=str, default=None,
         help='output filepath (default `stdout`)')
    padd('-e', '--encoding', type=str, default="cp932",
         help='output fileencoding (default `cp932`)')
    padd('-s', '--sort', action='store_false', default=True,
         help='Need Sort?')
    padd('-H', '--header', type=int, default=None,
         help='file no header (default `None`) (start sequence `0`)')
    padd('-k', '--key', type=str, default=None,
         help='key index of both filename1 and filename2  (ex. 3,2,4-8)')
    padd('-k1', '--key1', type=str, default=None,
         help='key index of filename1  (ex. 3,2,4-8)')
    padd('-k2', '--key2', type=str, default=None,
         help='key index of filename2  (ex. 3,2,4-8)')
    padd('-u', '--usecols', type=str, default=None,
         help='usecolumns of both filename1 and filename2  (ex. 1-8,10)')
    padd('-u1', '--usecols1', type=str, default=None,
         help='usecolumns of filename1  (ex. 1-8,10)')
    padd('-u2', '--usecols2', type=str, default=None,
         help='usecolumns of filename2  (ex. 1-8,10)')
    padd('-t', '--target', type=str, default=None,
         help='target table names or sheetname (ex. Sheet1, Sheet3)')
    padd('-t1', '--target1', type=str, default=None,
         help='target table names or sheetname of filename1 (ex. Sheet1, Sheet3)')
    padd('-t2', '--target2', type=str, default=None,
         help='target table names or sheetname of filename2 (ex. Sheet1, Sheet3)')
    args = ps.parse_args()

    usecols1 = kwtolist(args.usecols or args.usecols1)
    usecols2 = kwtolist(args.usecols or args.usecols2)

    def chktarget(tar):
        if tar and os.path.splitext(args.file1[0])[-1].lower() not in [".xlsx", ".xls", ".mdb", ".accdb"]:
            sys.stderr.write("`--target` option Only Excel or Access file")
            sys.exit(1)
        return kwtolist(tar)

    target1 = chktarget(args.target or args.target1)
    target2 = chktarget(args.target or args.target2)

    if args.key1 and args.key2:
        if len(kwtolist(args.key1)) != len(kwtolist(args.key2)):
            sys.stderr.write("Unmatch key count `--key1` vs `key2`")
            sys.exit(1)

    if target1:
        a = read_any(args.file1[0], target1, header=args.header, usecols=usecols1)
    else:
        a = read_any(args.file1[0], header=args.header, usecols=usecols1)

    if target2:
        b = read_any(args.file2[0], target2, header=args.header, usecols=usecols2)
    else:
        b = read_any(args.file2[0], header=args.header, usecols=usecols2)

    # TODO elegant
    if args.key1 and args.key2 and args.key1 != args.key2:
        cola = a.columns.tolist()
        colb = b.columns.tolist()
        ka = kwtolist(args.key1)
        kb = kwtolist(args.key2)
        ea = ka + values_not(cola, ka)
        eb = kb + values_not(colb, kb)
        if isposkey(ka) is False:
            ka = [cola.index(x) for x in ka]
            ea = ka + [cola.index(x) for x in values_not(cola, ka)]
        if isposkey(kb) is False:
            kb = [colb.index(x) for x in kb]
            eb = kb + [colb.index(x) for x in values_not(colb, kb)]

        a = a.iloc[:, ea]
        b = b.iloc[:, eb]
        if all(isinstance(k, int) for k in ka):
            a.columns = [str(x) for x in range(len(a.columns))]
            args.key1 = [str(x) for x in range(len(ka))]
        if all(isinstance(k, int) for k in kb):
            b.columns = [str(x) for x in range(len(b.columns))]
            args.key2 = [str(x) for x in range(len(kb))]

    f = codecs.open(args.outfile, mode="w", encoding=args.encoding) if args.outfile else sys.stdout

    writer = csv.writer(f, quoting=csv.QUOTE_ALL)

    if "ka" in locals():
        def render(val):
            ret = val[3:]
            for i, j, x in zip(ka, range(len(ka)), val[3:]):
                ret[j] = ret[i]
                ret[i] = x
            writer.writerow(val[:3] + ret)
    else:
        render = writer.writerow

    for d in differ(a, b,
                    keya=kwtolist(args.key or args.key1),
                    keyb=kwtolist(args.key or args.key2),
                    sort=args.sort, skipequal=args.all is False):

        render(d)

"""
   TestCase below
"""
def test():
    from util.core import tdir
    from io import StringIO
    from collections import Counter

    def test_sanitize():
        assert(sanitize(None, 1) == "ADD ---> 1")
        assert(sanitize(1, None) == "1 ---> DEL")
        assert(sanitize("abc", "acc") == "abc ---> acc")
        assert(sanitize(list("abc"), list("acc")) == ['a', 'b ---> c', 'c'])
        assert(sanitize(list("abc"), list("ab")) == ['a', 'b', 'c ---> DEL'])
        assert(sanitize(list("ab"), list("abc")) == ['a', 'b', 'ADD ---> c'])
        assert(sanitize([1,2,3], [1,3,3]) == [1, '2 ---> 3', 3])
        assert(sanitize([1,2,3], [1,2,None]) == [1, 2, '3 ---> DEL'])
        assert(sanitize([1,None,3], [1,2,3]) == [1, 'ADD ---> 2', 3])

    def test_compare_build():
        a, b = [list("abc"), list("def")], [list("abc"), list("def")]
        c = compare_build(a, b, [0], [0])
        assert(c(a[0],b[0]) == 0)

        a, b = list("abc"), list("aac")
        assert(c(a,b) == 0)

        a, b = list("bbc"), list("abc")
        assert(c(a,b) == 1)

        a, b = list("abc"), list("bbc")
        assert(c(a,b) == -1)

    def test_iterdiff1D():
        r = list(iterdiff1D(enumerate(list("abc")), enumerate(list("abb"))))[0]
        assert(list(r) == ["replace", 2,2, "c ---> b"])

        r = list(iterdiff1D(enumerate(list("abc")), enumerate(list("abb")), skipequal=False))
        assert(len(r) == 3)
        assert("equal" in [rr.tag for rr in r])

        r = list(iterdiff1D(enumerate(list("abc")), enumerate(list("ab")), na_value="--"))
        assert(len(r) == 1)
        assert(r[0].tag == "delete" and r[0].indexb == "--")


    def test_iterdiff2D():
        a = iterrows([list("abc"), list("def")])
        b = iterrows([list("abc"), list("ddf")])
        c = compare_build(a,b, [0], [0])
        r = list(iterdiff2D(a, b, c))[0]
        assert(r.tag == "replace")
        assert(r.value == ['d', 'e ---> d', 'f'])

        a = iterrows([list("abc"), list("def")])
        b = iterrows([list("abc"), list("ddf")])
        c = compare_build(a,b, [1], [1])
        r = list(iterdiff2D(a, b, c))
        assert(len(r) == 2)
        assert(set([r[0].tag, r[1].tag]) == set(["insert", "delete"]))

    def test_helperdiff1D():
        a, b = list("bac"), list("abb")
        assert(list(helperdiff1D(a, b)) == [
                ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'VALUE'],
                ['replace', 3, 3, 'c ---> b']]
            )
        assert(list(helperdiff1D(a, b, sort=False)) == [
                ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'VALUE'],
                ['replace', 1, 1, 'b ---> a'],
                ['replace', 2, 2, 'a ---> b'],
                ['replace', 3, 3, 'c ---> b']]
            )
        assert(list(helperdiff1D(a, b, skipequal=False)) == [
                ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'VALUE'],
                ['equal', 2, 1, 'a'],
                ['equal', 1, 2, 'b'],
                ['replace', 3, 3, 'c ---> b']]
            )
        assert(list(helperdiff1D(a, b, skipequal=False, startidx=0)) == [
                ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'VALUE'],
                ['equal', 1, 0, 'a'],
                ['equal', 0, 1, 'b'],
                ['replace', 2, 2, 'c ---> b']]
            )

    def test_helperdiff2D():
        a = [list("abc"), list("def")]
        b = [list("abc"), list("ddf")]
        assert(list(helperdiff2D(a, b)) == [
                ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'a', 'b', 'c'],
                ['insert', '', 2, 'd', 'd', 'f'],
                ['delete', 2, '', 'd', 'e', 'f']]
            )

        assert(list(helperdiff2D(a, b, skipequal=False)) == [
                ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'a', 'b', 'c'],  # not equal
                ['insert', '', 2, 'd', 'd', 'f'],
                ['delete', 2, '', 'd', 'e', 'f']]
            )
        h = [["col1", "col2", "col3"]]
        r = helperdiff2D(h + a, h + b, keya=[0], skipequal=False)
        assert(list(r)[1:] == [["equal",2,2,"a","b","c"],["replace",3,3,"d","e ---> d","f"]])

        r = helperdiff2D(h + a, h + b, keya=[1], skipequal=False)
        assert(list(r)[1:] == [["equal",2,2,"a","b","c"],["insert","",3, "d","d","f"],["delete",3,"","d","e","f"]])

        r = helperdiff2D(h + a, h + b, keya=["col1"], skipequal=False)
        assert(list(r)[1:] == [["equal",2,2,"a","b","c"],["replace",3,3,"d","e ---> d","f"]])

        r = helperdiff2D(h + a, h + b, keya=["col2"], skipequal=False)
        assert(list(r)[1:] == [["equal",2,2,"a","b","c"],["insert","",3, "d","d","f"],["delete",3,"","d","e","f"]])

    def test_diffauto():
        from util import read_any
        f1 = tdir + "diff1.csv"
        f2 = tdir + "diff2.csv"
        a = read_any(f1)
        b = read_any(f2)

        anser = [
            ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'mpg', 'cyl', 'displ', 'hp', 'weight', 'accel', 'yr', 'origin', 'name'],
            ['replace', 2, 2, 'b ---> 10', '8', '307', '130', '3504', '12', '70', '1', 'chevrolet chevelle malibue'],
            ['insert', '', 16, '22', '6', '198', '95', '2833', '15.5', '70', '1', 'plymouth duster'],
            ['insert', '', 24, '26', '4', '121', '113', '2234', '12.5', '70', '2', 'bmw 2002'],
            ['replace', 38, 40, '14', '9 ---> 8', '351', '153', '4154', '13.5', '71', '1', 'ford galaxie 500'],
            ['replace', 47, 49, '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2001 ---> mercury capri 2000'],
            ['delete', 56, '', '25', '4', '97.5', '80', '2126', '17', '72', '1', 'dodge colt hardtop'],
            ]
        assert(sorted(diffauto(a, b), key=str) == sorted(anser, key=str))

    def test_differcsv():
        from util.dfutil import read_any
        from util.core import kwtolist
        f1 = tdir + "diff1.csv"
        f2 = tdir + "diff2.csv"
        usecols1 = [0,1,2,3,4,5]
        usecols2 = [0,1,2,3,4,5]

        a = read_any(f1, usecols=kwtolist(usecols1))
        b = read_any(f2, usecols=kwtolist(usecols2))

        assert(list(differ(a, b)) == [
            ['DIFFTAG','LEFTLINE#','RIGHTLINE#','mpg','cyl','displ','hp','weight','accel'],
            ['replace', 2, 2, 'b ---> 10', '8', '307', '130', '3504', '12'],
            ['insert', '', 16, '22', '6', '198', '95', '2833', '15.5'],
            ['insert', '', 24, '26', '4', '121', '113', '2234', '12.5'],
            ['replace', 38, 40, '14', '9 ---> 8', '351', '153', '4154', '13.5'],
            ['delete', 56, '', '25', '4', '97.5', '80', '2126', '17']]
        )

    def test_differxls():
        from util.dfutil import read_any
        f1 = tdir + "diff1.xlsx"
        f2 = tdir + "diff2.xlsx"

        a = read_any(f1)
        b = read_any(f2)
        anser = [
            ['DIFFTAG','LEFTLINE#','RIGHTLINE#','TARGET', 'mpg', 'cyl', 'displ', 'hp', 'weight', 'accel', 'yr', 'origin', 'name'],
            ['replace', 2, 2, 'diff1 ---> diff2', 'b ---> 10', '8', '307', '130', '3504', '12', '70', '1', 'chevrolet chevelle malibue'],
            ['insert', '', 16, 'diff1 ---> diff2', '22', '6', '198', '95', '2833', '15.5', '70', '1', 'plymouth duster'],
            ['insert', '', 24, 'diff1 ---> diff2', '26', '4', '121', '113', '2234', '12.5', '70', '2', 'bmw 2002'],
            ['replace', 38, 40, 'diff1 ---> diff2', '14', '9 ---> 8', '351', '153', '4154', '13.5', '71', '1', 'ford galaxie 500'],
            ['replace', 47, 49, 'diff1 ---> diff2', '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2001 ---> mercury capri 2000'],
            ['delete', 56, '', 'diff1 ---> diff2', '25', '4', '97.5', '80', '2126', '17', '72', '1', 'dodge colt hardtop']]

        assert(list(differ(a,b)) == anser)

    def test_differlist():
        anser = [['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'VALUE'],
        ['equal', 1, 1, 1],
        ['replace', 2, 2, '2 ---> 3'],
        ['replace', 3, 3, '3 ---> 4']]
        assert(list(differ([1,2,3], [1,3,4], skipequal=False)) == anser)

        anser = [['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'VALUE'], ['equal', 1, 1, 'a'], ['equal', 2, 2, 'b'], ['replace', 3, 3, 'c ---> d']]
        assert(list(differ(list("abc"), list("abd"), skipequal=False)) == anser)

    def test_differequal():
        from util import read_any
        f = tdir + "diff1.csv"
        df = read_any(f).head(5)
        assert(list(differ(df, df)) == [])
        lst = list(map(list, df.itertuples()))
        assert(all(tag == "equal" for i, (tag, *x) in enumerate(differ(lst, lst, skipequal=False)) if i > 0))

    def stdoutcapture(*args):
        sio = StringIO()
        sys.stdout = sio
        del sys.argv[1:]
        sys.argv.extend(args)
        main()
        sio.seek(0)
        sys.stdout = sys.__stdout__
        return sio

    def test_default_main():
        sio = stdoutcapture(tdir+"diff1.csv", tdir+"diff2.csv")
        assert(Counter(x.split(",")[0] for x in sio) == Counter({'"replace"': 3, '"insert"': 2, '"DIFFTAG"': 1, '"delete"': 1}))

    def test_all_main():
        sio = stdoutcapture("-a", tdir+"diff1.csv", tdir+"diff2.csv")
        assert(Counter(x.split(",")[0] for x in sio.readlines()) == Counter({'"equal"': 386, '"replace"': 3, '"insert"': 2, '"DIFFTAG"': 1, '"delete"': 1}))

    def test_encoding_main():
        sio = stdoutcapture("-e", "cp932", tdir+"diff1.csv", tdir+"diff2.csv")
        assert(Counter(x.split(",")[0] for x in sio) == Counter({'"replace"': 3, '"insert"': 2, '"DIFFTAG"': 1, '"delete"': 1}))

    def test_sort_main():
        sio = stdoutcapture("-s", tdir+"diff1.csv", tdir+"diff2.csv")
        sio.read()

    def test_target_main():
        sio = stdoutcapture("-t1", "diff1", "-t2", "diff2", tdir+"diff1.xlsx", tdir+"diff2.xlsx")
        assert(len(sio.readlines()) == 7)

    def test_header_main():
        sio = stdoutcapture("-H", "0" ,tdir+"diff1.csv", tdir+"diff2.csv")
        header = next(sio).strip().replace('"', "").split(",")
        assert(header == ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'mpg', 'cyl', 'displ', 'hp', 'weight', 'accel', 'yr', 'origin', 'name'])
        assert(Counter(x.split(",")[0] for x in sio) == Counter({'"replace"': 3, '"insert"': 2, '"delete"': 1}))

    def test_key_main():
        sio = stdoutcapture("-k", "1" ,tdir+"diff1.csv", tdir+"diff2.csv")
        assert(sio.readlines()[2] == '"delete","3","","b","8","307","130","3504","12","70","1","chevrolet chevelle malibue"\r\n')

        sio = stdoutcapture("-H", "0", "-k", "mpg" ,tdir+"diff1.csv", tdir+"diff2.csv")
        assert(sio.readlines()[2] == '"delete","2","","b","8","307","130","3504","12","70","1","chevrolet chevelle malibue"\r\n')

    def test_key1_2_main():
        sio = stdoutcapture("-k1", "3" ,"-k2", "4" ,tdir+"diff1.csv", tdir+"diff2.csv")
        for x in sio:
            if "replace" in x:
                assert(x == '"replace","205","3","4 ---> 8","20 ---> 10","130","102 ---> 307","3150 ---> 3504","15.7 ---> 12","76 ---> 70","2 ---> 1","volvo 245 ---> chevrolet chevelle malibue"\r\n')
                break

    def test_key1_2_name_main():
        sio = stdoutcapture("-H", "0" ,"-k1", "displ" ,"-k2", "hp" ,tdir+"diff1.csv", tdir+"diff2.csv")
        for x in sio:
            if "replace" in x:
                assert(x == '"replace","204","2","4 ---> 8","20 ---> 10","130","102 ---> 307","3150 ---> 3504","15.7 ---> 12","76 ---> 70","2 ---> 1","volvo 245 ---> chevrolet chevelle malibue"\r\n')
                break

    def test_usecol_main():
        sio = stdoutcapture("-u", "1,9" ,tdir+"diff1.csv", tdir+"diff2.csv")
        next(sio)
        for x in sio:
            assert(x == '"replace","3","3","b ---> 10","chevrolet chevelle malibue"\r\n')
            break

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            func()

if __name__ == "__main__":
    # test()
    main()
