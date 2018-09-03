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
        )

from util.profiler import Profile

import os
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

def compare_build(A, B, keya, keyb):
    def getfunc(o, key):
        if is1darray(o):
            return str
        elif isposkey(key):
            return lambda x: [x[i] for i in key]
        elif is2darray(o):
            return lambda x: [x[i] for i in [o[0].index(k) for k in key]]
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

    compare = compare_build(A, B, keya, keyb)
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
    diffkey = Profile(a[a.columns[3:]], top=None).diffkey
    for key in [[]] + list(diffkey.values()):
        if key == []:
            r = helperdiff2D(a, b, skipequal=skipequal, startidx=startidx)
            header = next(r)
            yield header
            reta, retb = [header[3:]], [header[3:]]
        else:
            r = helperdiff2D(reta.copy(), retb.copy(), keya=key, keyb=key, skipequal=skipequal, startidx="infer")
            reta, retb = [header[3:]], [header[3:]]

        for tag ,ia, ib, *content in r:
            if tag == "equal":
                yield [tag , ia, ib] + content
            elif tag == "replace":
                yield [tag , ia, ib] + content
            elif tag == "delete":
                reta.append([ia] + content)
            elif tag == "insert":
                retb.append([ib] + content)
    else:
        for x in reta[1:]:
            yield ["delete", x[0], "", *x[1:]]
        for x in retb[1:]:
            yield ["insert", "", x[0], *x[1:]]


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
                header = ["DIFFTAG","LEFTLINE#", "RIGHTLINE#"] + [A.columns.tolist()]
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
    import sys
    import csv

    def getrange(key):
        if not key:
            return
        ret = kwtolist(key)
        def columnslice(k):
            if not k:
                return
            try:
                return tuple(k[x] for x in ret)
            except IndexError:
                raise AttributeError(f"Invalid key Index number.\n {ret} Not in {k}")
        return columnslice

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
    padd('-s', '--sort', action='store_true', default=False,
         help='before diff file Sorted.')
    padd('-H', '--header', type=int, default=None,
         help='file no header (default `False`) (sorted option)')
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
        a = read_any(args.file1[0], target1, header=args.header, usecols1=usecols1)
    else:
        a = read_any(args.file1[0], header=args.header, usecols1=usecols1)

    if target2:
        b = read_any(args.file2[0], target2, header=args.header, usecols2=usecols2)
    else:
        b = read_any(args.file2[0], header=args.header, usecols2=usecols2)

    f = codecs.open(args.outfile, mode="w", encoding=args.encoding) if args.outfile else sys.stdout

    writer = csv.writer(f, quoting=csv.QUOTE_ALL)

    for d in differ(a, b,
                    keya=getrange(args.key or args.key1),
                    keyb=getrange(args.key or args.key2),
                    sort=args.sort, skipequal=args.all is False):

        writer.writerow(d)

"""
   TestCase below
"""
def test():
    from util.core import tdir
    import sys

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

    def test_diffauto():
        from util import read_any
        f1 = tdir + "diff1.csv"
        f2 = tdir + "diff2.csv"
        a = read_any(f1)
        b = read_any(f2)
        assert(list(diffauto(a,b)) == [
            ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'mpg', 'cyl', 'displ', 'hp', 'weight', 'accel', 'yr', 'origin', 'name'],
            ['replace', 2, 2, 'b ---> 10', '8', '307', '130', '3504', '12', '70', '1', 'chevrolet chevelle malibue'],
            ['replace', 38, 40, '14', '9 ---> 8', '351', '153', '4154', '13.5', '71', '1', 'ford galaxie 500'],
            ['delete', 56, '', '25', '4', '97.5', '80', '2126', '17', '72', '1', 'dodge colt hardtop'],
            ['delete', 47, '', '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2001'],
            ['insert', '', 24, '26', '4', '121', '113', '2234', '12.5', '70', '2', 'bmw 2002'],
            ['insert', '', 49, '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2000'],
            ['insert', '', 16, '22', '6', '198', '95', '2833', '15.5', '70', '1', 'plymouth duster']]
        ) #TODO result sorted

        assert(list(diffauto(a,b, startidx=0))[1][1:3] == [1,1])

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

        a = read_any(f1,0)
        b = read_any(f2,0)

        print("exists bugs:", list(differ(a,b)) == [
            ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'mpg', 'cyl', 'displ', 'hp', 'weight', 'accel', 'yr', 'origin', 'name'],
            ['replace', 2, 2, 'b ---> 10', '8', '307', '130', '3504', '12', '70', '1', 'chevrolet chevelle malibue'],
            ['insert', '', 16, '22', '6', '198', '95', '2833', '15.5', '70', '1', 'plymouth duster'],
            ['insert', '', 24, '26', '4', '121', '113', '2234', '12.5', '70', '2', 'bmw 2002'],
            ['replace', 38, 40, '14', '9 ---> 8', '351', '153', '4154', '13.5', '71', '1', 'ford galaxie 500'],
            ['replace', 47, 49, '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2001 ---> mercury capri 2000'], #TODO bug
            ['delete', 56, '', '25', '4', '97.5', '80', '2126', '17', '72', '1', 'dodge colt hardtop']]
        )

    def nontest_differlist():
        for x in differ([1,2,3], [1,3,4], skipequal=False):
            print(x)
        for x in differ(list("abc"), list("abd"), skipequal=False):
            print(x)

    def nontest_differequal():
        from util import read_any
        f = tdir + "diff1.csv"
        df = read_any(f).head(5)
        assert(list(differ(df, df)) == [])
        lst = list(map(list, df.itertuples()))
        print(list(differ(lst, lst, skipequal=False)))


    def nontest_main():
        sys.argv.extend([tdir+"diff1.csv", tdir+"diff2.csv"])
        main()

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            func()


if __name__ == "__main__":
    test()
#    main()