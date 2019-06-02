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
        isposkey,
        isdataframe,
        sortedrows,
        iterrows,
        flatten,
        sorter,
        kwtolist,
        values_at,
        values_not,

        )

from util.profiler import Profile
from pandas import DataFrame

import os
import sys
from itertools import chain, zip_longest
from collections import namedtuple
import math


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
    if isinstance(a, (tuple, list)) and isinstance(b, (tuple, list)):
        return [comp(*x) for x in zip_longest(a, b, fillvalue="")]
    else:
        return comp(a, b)

def tokey(key=None, default=lambda x: x, columns=None):
    """
    Return: sorted 2d generator -> tuple(rownumber, row)
    """
    if not key:
        return default

    if callable(key):
        return key

    if columns:
        try:
            pos = [columns.index(x) for x in key]
        except ValueError:
            try:
                pos = [columns.index(str(x)) for x in key]
            except ValueError:
                pos = None

        if pos:
            def values_at_key(x):
                if x:
                    try:
                        return values_at(x, pos)
                    except ValueError:
                        raise ValueError("Unknown column is `{}`.\n            But key not in `{}`.".format(columns, key))

            return values_at_key

    def itemkey(x):
        if x:
            return [x[k] for k in key]

    return itemkey

dinfo = namedtuple("DiffInfo", ("tag", "indexa", "indexb", "valuea", "valueb"))

class Differ(object):
    render_header = ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#']

    def __init__(self, a, b, skipequal=True, sorted=False, keya=None, keyb=None,
                 startidx=1, header=None, usecola=None, usecolb=None,
                 na_value="", callback=flatten):
        self.org_a = a
        self._a = None
        self.org_b  = b
        self._b = None
        self.skipequal = skipequal
        self.sorted = sorted
        self._keya = keya
        self._keyb = keyb
        self.startidx = startidx
        self.header = header
        self._usecola = usecola
        self._usecolb = usecolb
        self.na_value= na_value
        self.callback = callback

        self.existskey = (keya or keyb) is not None
        self._keya_func = self._keyb_func = None
        self._header_a = self._header_b = None
        self.existsusecol = (usecola or usecolb) is not None
        self._usecola_func = self._usecolb_func = None

        self.unmatch_count_allow = 1
        self._compute = None

    @property
    def a(self):
        if self._a is None:
            if self.sorted:
                self._a = iterrows(self.org_a, start=self.startidx)
            else:
                self._a = sortedrows(self.org_a, key=self._keya, start=self.startidx, header=self.header)

            if self.header:
                self.header_a

        return self._a

    @property
    def b(self):
        if self._b is None:
            if self.sorted:
                self._b = iterrows(self.org_b, start=self.startidx)
            else:
                self._b = sortedrows(self.org_b, key=self._keyb, start=self.startidx, header=self.header)

            if self.header:
                self.header_b

        return self._b

    @property
    def header_a(self):
        if self._header_a is None and self.header:
            self._header_a = next(self.a)[1]
        return self._header_a

    @property
    def header_b(self):
        if self._header_b is None and self.header:
            self._header_b = next(self.b)[1]
        return self._header_b

    @property
    def keya(self):
        if self._keya_func is None:
            self._keya_func = tokey(self._keya, columns=self.header_a)
        return self._keya_func

    @property
    def keyb(self):
        if self._keyb_func is None:
            self._keyb_func = tokey(self._keyb, columns=self.header_b)
        return self._keyb_func

    @property
    def usecola(self):
        if self._usecola_func is None:
            self._usecola_func = tokey(self._usecola, columns=self.header_a)
        return self._usecola_func

    @property
    def usecolb(self):
        if self._usecolb_func is None:
            self._usecolb_func = tokey(self._usecolb, columns=self.header_b)
        return self._usecolb_func

    def itertuples(self):
        ia, ib = True, True

        while True:
            if ia:
                na, a = next(self.a, [None, None])
                use_a = self.usecola(a)
            if ib:
                nb, b = next(self.b, [None, None])
                use_b = self.usecolb(b)
            if a is None and b is None:
                break
            if b is None:
                yield dinfo("delete", na, self.na_value, a, b)
                ia, ib = True, False
            elif a is None:
                yield dinfo("insert", self.na_value, nb, a, b)
                ia, ib = False, True
            elif a == b:
                yield dinfo("equal", na, nb, a, b)
                ia, ib = True, True
            else:
                fx = self.keya(a)
                fy = self.keyb(b)

                if self.existskey:
                    if fx == fy:
                        yield dinfo("replace", na, nb, a, b)
                        ia, ib = True, True
                        continue
                elif isinstance(a, int) or isinstance(b, int):
                    pass
                else:
                    if use_a == use_b or \
                        len(a) + len(b) > self.unmatch_count_allow * 2 and \
                        sum(1 for x in zip_longest(a, b) if x[0] != x[1]) <= self.unmatch_count_allow:
                        yield dinfo("replace", na, nb, a, b)
                        ia, ib = True, True
                        continue

                if fx < fy:
                    yield dinfo("delete", na, self.na_value, a, b)
                    ia, ib = True, False
                elif fx > fy:
                    yield dinfo("insert", self.na_value, nb, a, b)
                    ia, ib = False, True
                else:
                    yield dinfo("replace", na, nb, a, b)
                    ia, ib = True, True

    def compute(self, callback=None):
        callback = callback or self.callback

        for i, t in enumerate(self.itertuples()):
            if i == 0 and self.header:
                r = sanitize(self.usecola(self.header_a),
                        self.usecolb(self.header_b))
                yield callback([self.render_header, r])

            if self.skipequal and t.tag == "equal":
                continue

            if t.tag in ["equal", "delete"]:
                r = self.usecola(t.valuea)
            elif t.tag == "insert":
                r = self.usecolb(t.valueb)
            else:
                r = sanitize(self.usecola(t.valuea),
                    self.usecolb(t.valueb))

            yield callback([t[:3], r])

    def __next__(self):
        if self._compute is None:
            self._compute = self.compute()
        return next(self._compute)

    def __iter__(self):
        if self._compute is None:
            self._compute = self.compute()
        return self._compute

def diffauto(a, b, sorted=False, skipequal=True, startidx=1, header=True, usecola=None, usecolb=None):
    cola = a.columns.tolist()
    colb = b.columns.tolist()
    diffkeys = [(None, None)]

    if sorted is False:
        dka = {tuple(cola.index(z) for z in v):k for k, v in Profile(a.head(10), top=None).diffkey.items()}
        dkb = {tuple(colb.index(z) for z in v) for v in Profile(b.head(10), top=None).diffkey.values()}
        dk=[(dka[x], x) for x in set(dka) & dkb]
        dk.sort()
        diffkeys += dk
    dummy = -1

    for i, (_, key) in enumerate(diffkeys):
        if i == 0:
            r = Differ(a, b, sorted=sorted, skipequal=skipequal, startidx=startidx, header=True, usecola=usecola, usecolb=usecolb)
            r.unmatch_count_allow = math.floor(math.log(len(cola))*math.log(len(colb))) if sorted else 0
            reta, retb = [[dummy, *cola]], [[dummy, *colb]]
        else:
            r = Differ(reta.copy(), retb.copy(), keya=key, keyb=key, skipequal=skipequal, startidx="infer", header=True, usecola=usecola, usecolb=usecolb)
            reta, retb = [[dummy, *cola]], [[dummy, *colb]]
        for j, x in enumerate(r):
            if i + j == 0 and header:
                yield x

            if dummy in [x[1], x[2]]:
                continue
            elif x[0] in ["equal", "replace"]:
                yield x
            elif x[0] == "delete":
                reta.append([x[1], *x[3:]])
            elif x[0] == "insert":
                retb.append([x[2], *x[3:]])
    else:
        #Last Loop
        for y in reta:
            if y[0] != dummy:
                yield ["delete", y[0], "", *y[1:]]
        for y in retb:
            if y[0] != dummy:
                yield ["insert", "", y[0], *y[1:]]

def dictdiffer(a, b, keya=None, keyb=None, sorted=False, skipequal=True, startidx=1, header=True, usecola=None, usecolb=None):
    r = Differ(list(a), list(b), sorted=True, skipequal=False)
    i = 0

    for tag, ll, rl, val in r:
        if tag == "equal":
            _a, _b = a[val], b[val]
        elif tag == "replace":
            va, vb = val.split(" ---> ")
            _a, _b = a[va], b[vb]
        elif tag == "insert":
            _a, _b = DataFrame(), b[val]
        elif tag == "delete":
            _a, _b = a[val], DataFrame()

        rr = diffauto(_a, _b, sorted=sorted, skipequal=skipequal, startidx=startidx, header=header, usecola=usecola, usecolb=usecolb)

        for d in rr:
            d.insert(3, "TARGET" if i == 0 and header else val)
            yield d
            i += 1

def differ(A, B, keya=None, keyb=None, sorted=False, skipequal=True, startidx=1, header=True, usecola=None, usecolb=None):
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
            if ck and not skipequal:
                header = [["DIFFTAG","LEFTLINE#", "RIGHTLINE#"] + A[0]]
                return chain(header, (flatten(["equal", i, i, x]) for i, x in enumerate(A)))

        if ck and skipequal:
            return []
    except:
        pass
    if isinstance(A, dict) and isinstance(B, dict):
        ret = dictdiffer(A, B, keya=keya, keyb=keyb, sorted=sorted, skipequal=skipequal, startidx=startidx, header=header, usecola=usecola, usecolb=usecolb)
    elif isdataframe(A) and isdataframe(B) and not keya and not keyb:
        ret = diffauto(A, B, skipequal=skipequal, startidx=startidx, header=header, usecola=usecola, usecolb=usecolb)
    else:
        ret = Differ(A, B, keya=keya, keyb=keyb, sorted=sorted, skipequal=skipequal, startidx=startidx, header=header, usecola=usecola, usecolb=usecolb)

    if sorted is False:
        def sortout(x):
            if x[1] and x[2]:
                r = min(x[1:3])
            else:
                r = x[1] or x[2]
            return r if isinstance(r, int) else -1

        if header:
            head = [next(ret)]
            return chain(head, sorter(ret, key=sortout))
        else:
            return sorter(ret, key=sortout)
    else:
        return ret

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
    padd('--sep', type=str, default=",",
         help='output separator (default ,)')
    padd('-s', '--sorted', action='store_true', default=False,
         help='Input data sorted?')
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
        if any(isinstance(k, int) for k in ka):
            a.columns = [str(x) for x in range(len(a.columns))]
            args.key1 = [str(x) for x in range(len(ka))]
        if any(isinstance(k, int) for k in kb):
            b.columns = [str(x) for x in range(len(b.columns))]
            args.key2 = [str(x) for x in range(len(kb))]

    r = differ(a, b,
                    keya=kwtolist(args.key or args.key1),
                    keyb=kwtolist(args.key or args.key2),
                    sorted=args.sorted, skipequal=args.all is False,
                    usecola=usecols1, usecolb=usecols2,
                    )

    if args.outfile and os.path.splitext(args.outfile)[1].lower().startswith(".xls"):
        col = next(r)
        return DataFrame(r, columns=col).to_excel_plus(args.outfile,
                         conditional_value="* ---> *",
                         title="2Diff {} vs {}".format(args.file1[0],args.file2[0])
                         )

    f = codecs.open(args.outfile, mode="w", encoding=args.encoding) if args.outfile else sys.stdout
    writer = csv.writer(f, delimiter=args.sep, quoting=csv.QUOTE_ALL)

    if "ka" in locals():
        def render(val):
            ret = val[3:]
            for i, j, x in zip(ka, range(len(ka)), val[3:]):
                ret[j] = ret[i]
                ret[i] = x
            writer.writerow(val[:3] + ret)
    else:
        render = writer.writerow

    for d in r:
        render(d)

def test():
    from util.core import tdir
    from io import StringIO
    from collections import Counter
    from util.dfutil import read_any
    from datetime import datetime as dt

    pe = lambda *x: print("\n", *x, file=sys.stderr)

    def debug_test_Differ_usecol():
        import pandas as pd
        a = [list("ac"), list("13"), list("46")]
        b = [list("ac"), list("13"), list("56")]
        a = pd.DataFrame(a[1:], columns=a[0])
        r = list(differ(a,b,usecola=["a","c"],usecolb=["a","c"], header=True))
        pe(r)

    def test_usecol_main():
        sio = stdoutcapture("-u", "1,9" ,tdir+"diff1.csv", tdir+"diff2.csv")
        next(sio)
        for x in sio:
            assert(x == '"replace","3","3","b ---> 10","chevrolet chevelle malibue"\r\n')
            break

    def test_usecol1_2_main():
        sio = stdoutcapture("-u1", "1,9", "-u2", "1,9" ,tdir+"diff1.csv", tdir+"diff2.csv")
        next(sio)
        assert(Counter([x.split(",")[0] for x in sio.readlines()]) == Counter({'"replace"': 2, '"insert"': 2, '"delete"': 1}))

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

    def test_tokey():
        assert(tokey([0,1,2]).__name__ == "itemkey")
        assert(tokey(list("abc")).__name__ == "itemkey")
        assert(tokey(list("abc"),columns=list("abc")).__name__ == "values_at_key")
        assert(tokey().__name__ == "<lambda>")

    def test_Differ():
        a = list("abc")
        b = list("abb")

        r = list(Differ(a, b))
        assert(r == [["insert", "", 3, "b"], ["delete", 3, "", "c"]])
        r = list(Differ(iter(a), iter(b)))
        assert(r == [["insert", "", 3, "b"], ["delete", 3, "", "c"]])
        r = list(Differ((x for x in a), (x for x in b)))
        assert(r == [["insert", "", 3, "b"], ["delete", 3, "", "c"]])
        r = list(Differ(a, b,keya=lambda x: True, keyb=lambda x:True))
        assert(r == [["replace",3,3,"c ---> b"]])

    def test_Differ_1D():
        a = list("cba")
        b = list("bba")
        r = list(Differ(a, b, sorted=False))
        assert(r == [["insert", "", 2, "b"], ["delete", 1, "", "c"]])
        r = list(Differ(a, b, sorted=True))
        assert(r != [["insert", "", 2, "b"], ["delete", 1, "", "c"]])
        assert(len(r) == 6)

        r = list(Differ(iter(a), iter(b), sorted=False))
        assert(r == [["insert", "", 2, "b"], ["delete", 1, "", "c"]])
        r = list(Differ(iter(a), iter(b), sorted=True))
        assert(r != [["insert", "", 2, "b"], ["delete", 1, "", "c"]])
        assert(len(r) == 6)

    def test_Differ_2D_list():
        a = [list("abc"), list("def"), list("ghi")]
        b = [list("abc"), list("dff"), list("xyz")]

        r = list(Differ(a, b))
        assert(r == [
                ['replace', 2, 2, 'd', 'e ---> f', 'f'],
                ['delete', 3, '', 'g', 'h', 'i'],
                ['insert', '', 3, 'x', 'y', 'z']
                ]
            )

    def test_Differ_2D_iter():
        a = [list("abc"), list("def"), list("ghi")]
        b = [list("abc"), list("dff"), list("xyz")]
        r = list(Differ(iter(a), iter(b)))
        assert(r == [
                ['replace', 2, 2, 'd', 'e ---> f', 'f'],
                ['delete', 3, '', 'g', 'h', 'i'],
                ['insert', '', 3, 'x', 'y', 'z']
                ]
            )

    def test_Differ_2D_enum():
        a = [list("abc"), list("def"), list("ghi")]
        b = [list("abc"), list("dff"), list("xyz")]
        r = list(Differ(enumerate(a), enumerate(b)))
        assert(r == [['replace', 2, 2, 1, "['d', 'e', 'f'] ---> ['d', 'f', 'f']"],
                      ['replace', 3, 3, 2, "['g', 'h', 'i'] ---> ['x', 'y', 'z']"]
                      ])

    def test_Differ_2D_key():
        a = [list("abc"), list("def"), list("ghi")]
        b = [list("abc"), list("dff"), list("xyz")]

        r = list(Differ(a, b, keya=lambda x: x[1], keyb=lambda x:x[1]))
        assert(r == [['delete', 2, '', 'd', 'e', 'f'],
                    ['insert', '', 2, 'd', 'f', 'f'],
                    ['delete', 3, '', 'g', 'h', 'i'],
                    ['insert', '', 3, 'x', 'y', 'z']
                    ])

    def test_Differ_df():
        import pandas as pd
        df1 = pd.DataFrame([
            dict(a=1,c=1),
            dict(a=1,c=4),
            dict(a=1,c=4),
            dict(a=3,c=2),
            ]
            )
        df2 = pd.DataFrame([
            dict(a=1,c=3),
            dict(a=1,c=4),
            dict(a=1,c=4),
            dict(a=2,c=1),
            dict(a=4,c=2),
            ]
            )
        r = list(Differ(df1, df2, skipequal=False, sorted=True, keya=[0], keyb=[0], header=True))
        assert(r == [
            ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'a', 'c'],
            ['replace', 2, 2, 1, '1 ---> 3'],
            ['equal', 3, 3, 1, 4],
            ['equal', 4, 4, 1, 4],
            ['insert', '', 5, 2, 1],
            ['delete', 5, '', 3, 2],
            ['insert', '', 6, 4, 2]
            ]
            )

    def test_diffauto():
        f1 = tdir + "diff1.csv"
        f2 = tdir + "diff2.csv"
        a = read_any(f1)
        b = read_any(f2)

        r = diffauto(a, b)

        anser = [
            ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'mpg', 'cyl', 'displ', 'hp', 'weight', 'accel', 'yr', 'origin', 'name'],
            ['replace', 2, 2, 'b ---> 10', '8', '307', '130', '3504', '12', '70', '1', 'chevrolet chevelle malibue'],
            ['insert', '', 16, '22', '6', '198', '95', '2833', '15.5', '70', '1', 'plymouth duster'],
            ['insert', '', 24, '26', '4', '121', '113', '2234', '12.5', '70', '2', 'bmw 2002'],
            ['replace', 38, 40, '14', '9 ---> 8', '351', '153', '4154', '13.5', '71', '1', 'ford galaxie 500'],
            ['replace', 47, 49, '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2001 ---> mercury capri 2000'],
            ['delete', 56, '', '25', '4', '97.5', '80', '2126', '17', '72', '1', 'dodge colt hardtop'],
            ]
        assert(sorted(r, key=str) == sorted(anser, key=str))

    def test_dictdiffer():
        a = read_any(tdir + "diff1.xlsx")
        b = read_any(tdir + "diff2.xlsx")

        r = list(dictdiffer(a, b))

        assert(sorted(r) == sorted([
                ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'TARGET', 'mpg', 'cyl', 'displ', 'hp', 'weight', 'accel', 'yr', 'origin', 'name'],
                ['replace', 47, 49, 'diff1 ---> diff2', '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2001 ---> mercury capri 2000'],
                ['replace', 38, 40, 'diff1 ---> diff2', '14', '9 ---> 8', '351', '153', '4154', '13.5', '71', '1', 'ford galaxie 500'],
                ['replace', 2, 2, 'diff1 ---> diff2', 'b ---> 10', '8', '307', '130', '3504', '12', '70', '1', 'chevrolet chevelle malibue'],
                ['delete', 56, '', 'diff1 ---> diff2', '25', '4', '97.5', '80', '2126', '17', '72', '1', 'dodge colt hardtop'],
                ['insert', '', 24, 'diff1 ---> diff2', '26', '4', '121', '113', '2234', '12.5', '70', '2', 'bmw 2002'],
                ['insert', '', 16, 'diff1 ---> diff2', '22', '6', '198', '95', '2833', '15.5', '70', '1', 'plymouth duster']
                ])
            )

    def test_differcsv():

        f1 = tdir + "diff1.csv"
        f2 = tdir + "diff2.csv"
        usecols1 = [0,1,2,3,4,5]
        usecols2 = [0,1,2,3,4,5]

        a = read_any(f1, usecols=kwtolist(usecols1))
        b = read_any(f2, usecols=kwtolist(usecols2))
        r = list(differ(a, b, header=True))

        assert(r == [
            ['DIFFTAG','LEFTLINE#','RIGHTLINE#','mpg','cyl','displ','hp','weight','accel'],
            ['replace', 2, 2, 'b ---> 10', '8', '307', '130', '3504', '12'],
            ['insert', '', 16, '22', '6', '198', '95', '2833', '15.5'],
            ['insert', '', 24, '26', '4', '121', '113', '2234', '12.5'],
            ['replace', 38, 40, '14', '9 ---> 8', '351', '153', '4154', '13.5'],
            ['delete', 56, '', '25', '4', '97.5', '80', '2126', '17']]
        )

    def test_differxls():
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

    def test_differxls_blanksheet():
        f1 = tdir + "diff5.xlsx"
        f2 = tdir + "diff6.xlsx"

        a = read_any(f1)
        b = read_any(f2)
        anser = [
            ['DIFFTAG', 'LEFTLINE#', 'RIGHTLINE#', 'TARGET', 'mpg', 'cyl', 'displ', 'hp', 'weight', 'Unnamed: 5', 'accel', 'yr', 'origin', 'name'],
            ['replace', 2, 2, 'diff1 ---> diff2', 'b ---> 10', '8', '307', '130', '3504', '', '12', '70', '1', 'chevrolet chevelle malibue'],
            ['insert', '', 16, 'diff1 ---> diff2', '22', '6', '198', '95', '2833', '', '15.5', '70', '1', 'plymouth duster'],
            ['insert', '', 24, 'diff1 ---> diff2', '26', '4', '121', '113', '2234', '', '12.5', '70', '2', 'bmw 2002'],
            ['replace', 38, 40, 'diff1 ---> diff2', '14', '9 ---> 8', '351', '153', '4154', '', '13.5', '71', '1', 'ford galaxie 500'],
            ['replace', 47, 49, 'diff1 ---> diff2', '23', '4', '122', '86', '2220', '', '14', '71', '1', 'mercury capri 2001 ---> mercury capri 2000'],
            ['delete', 56, '', 'diff1 ---> diff2', '25', '4', '97.5', '80', '2126', '', '17', '72', '1', 'dodge colt hardtop']]

        assert(list(differ(a,b)) == anser)

    def test_differlist():
        anser = [['equal', 1, 1, 1],
        ['replace', 2, 2, '2 ---> 3'],
        ['replace', 3, 3, '3 ---> 4']]
        r = list(differ([1,2,3], [1,3,4], header=None, skipequal=False,keya=lambda x:True, keyb=lambda x:True))
        assert(r == anser)

        anser = [['equal', 1, 1, 'a'], ['equal', 2, 2, 'b'], ['replace', 3, 3, 'c ---> d']]
        r = list(differ(list("abc"), list("abd"), header=None, skipequal=False,keya=lambda x:True, keyb=lambda x:True))
        assert(r == anser)

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

    def test_target_common_main():
        sio = stdoutcapture("-t", "Sheet1",tdir+"diff3.xlsx", tdir+"diff4.xlsx")
        next(sio)
        assert(Counter([x.split(",")[0] for x in sio.readlines()]) == Counter({'"replace"': 3, '"insert"': 2, '"delete"': 1}))

    def test_target1_2_main():
        sio = stdoutcapture("-t1", "diff1", "-t2", "diff2" ,tdir+"diff3.xlsx", tdir+"diff4.xlsx")
        next(sio)
        assert(Counter([x.split(",")[0] for x in sio.readlines()]) == Counter({'"replace"': 3, '"insert"': 2, '"delete"': 1}))

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = dt.now()
            func()
            t2 = dt.now()
            print("{} : time {}".format(x, t2-t1),file=sys.stderr)


if __name__ == "__main__":
#    test()
    main()
