#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library

MIT License

"""

__version__ = "0.4.0"
__author__ = "m.yama"


__all__ = ["differ"]

from functools import  lru_cache
from itertools import zip_longest
import re
from operator import itemgetter
import sys
from itertools import tee


try:
    from util.libs.similar import similar, flatten, sanitize, deephash

    from functools import  _CacheInfo, _lru_cache_wrapper
    similar = _lru_cache_wrapper(similar, 128, False, _CacheInfo)
except:
    from collections import _count_elements
    from collections import defaultdict

    BASE_TYPE = [type(None), int, float, str, bytes, bytearray, bool]

    @lru_cache(maxsize=16)
    def compare(x, y, conditional_value=' ---> ', delstr='DEL', addstr='ADD'):
        if x == y:
            return x
        elif x and y:
            return "{}{}{}".format(x, conditional_value, y)
        elif x:
            return "{}{}{}".format(x, conditional_value, delstr)
        else:
            return "{}{}{}".format(addstr, conditional_value, y)

    def sanitize(a, b, **kw):
        if type(a) in BASE_TYPE and type(b) in BASE_TYPE:
            return compare(a, b, **kw)
        else:
            return [compare(x, y, **kw) for x, y in zip_longest(a, b, fillvalue="")]

    def flatten(x):
        try:
            result = []
            for y in x:
                if type(y) in BASE_TYPE:
                    result.append(y)
                else:
                    result.extend(flatten(y))
            return tuple(result)
        except TypeError:
            return (x, )

    def deephash(x):
        try:
            return tuple([hash(y) if type(y) in BASE_TYPE else deephash(y) for y in x])
        except:
            return (hash(x), )

    def snake(k:int, y:int, left:object, right:object):
        x = y - k
        while x < len(left) and y < len(right) and left[x] == right[y]:
            x += 1
            y += 1
        return y

    @lru_cache()
    def similar(left, right):
        """
            Parameters:
                left: ex.tuple (Compare target data left)
                right: ex.tuple (Compare target data right)
            Return:
                float (0.0 < return <= 1.000000000002)
        """
        if left == right:
            return 1.0

        if not (left and right):
            return 0.0

        try:
            l1, l2 = len(left), len(right)
            o1, o2 = left, right
        except:
            o1 = list(left)
            o2 = list(right)
            l1, l2 = len(o1), len(o2)

        s1 = o2 if l1 > l2 else o1
        s2 = o1 if l1 > l2 else o2
        sl1 = len(s1)
        sl2 = len(s2)
        fp = defaultdict(lambda:-1)
        offset = sl1 + 1
        delta = sl2 - sl1

        p = 0
        while fp[delta + offset] != sl2:
            for k in range(-p, delta):
                fp[k + offset] = snake(k, max(fp[k-1+offset] + 1, fp[k+1+offset]), s1, s2)
            for k in range(delta + p, delta, -1):
                fp[k + offset] = snake(k, max(fp[k-1+offset] + 1, fp[k+1+offset]), s1, s2)
            fp[delta + offset] = snake(delta, max(fp[delta-1+offset] + 1, fp[delta+1+offset]), s1, s2)

            p += 1
        ed = delta + (p - 1)
        return 1.0 - (ed / max(l1,l2))


def countby(seq, func=None, return_index=False):
    result = {}
    if func:
        if return_index:
            indexes = {}
            for value in seq:
                key = func(value)
                if key in result:
                    result[key] += 1
                else:
                    result[key] = 1
                    indexes[key] = value
            return result, indexes
        else:
            for value in seq:
                key = func(value)
                if key in result:
                    result[key] += 1
                else:
                    result[key] = 1
    else:
        _count_elements(result, seq)
    return result

def groupby(seq, func=None, return_index=False, startidx=0):
    result = {}
    if func:
        if return_index:
            indexes = {}
            for i, value in enumerate(seq, startidx):
                key = func(value)
                if key in result:
                    result[key].append(i)
                else:
                    result[key] = [i]
                    indexes[key] = value
            return result, indexes
        else:
            for i, value in enumerate(seq):
                key = func(value)
                if key in result:
                    result[key].append(i)
                else:
                    result[key] = [i]
    else:
        for i, key in enumerate(seq):
            if key in result:
                result[key].append(i)
            else:
                result[key] = [i]
    return result

def differ(a, b, header=False, diffonly=False, sort=True, reverse=False, rep_rate=0.6, na_val=None, startidx=0, **kw):
    result = []

    if hasattr(a, "__next__"):
        a, _a = tee(a)
        ca = countby(_a, deephash)
    else:
        ca = countby(a, deephash)

    if hasattr(b, "__next__"):
        b, _b = tee(b)
        cb = countby(_b, deephash)
    else:
        cb = countby(b, deephash)

    ga, ia = groupby(a, deephash, return_index=True, startidx=startidx)
    gb, ib = groupby(b, deephash, return_index=True, startidx=startidx)

    cab = ca.keys() & cb.keys()

    ra = {k:ga[k] for k in (ga.keys() - cab)}
    rb = {k:gb[k] for k in (gb.keys() - cab)}

    for k in cab:
        if not diffonly:
            val = ia[k]
            result.extend([flatten(("equal", x, y, val)) for x, y in zip(ga[k], gb[k])])

        i, j = ca[k], cb[k]
        if i < j:
            rb[k] = gb[k][i:]
        elif i > j:
            ra[k] = ga[k][j:]

    del cab, ca, cb, ga, gb

    if 0 < rep_rate and rep_rate < 1:
        for repa, ida in ra.items():
            rate = -1.0
            ret = None
            for repb, idb in rb.items():
                r = similar(repa, repb)
                if r < rate:
                    continue
                rate = r
                ret = (repb, idb)
            if rate < rep_rate:
                val = ia[repa]
                result.extend([flatten(("delete", x, na_val, val)) for x in ida])
                continue

            repb, idb = ret

            val = sanitize(ia[repa], ib[repb])
            z = list(zip(ida, idb))
            result.extend([flatten(("replace", x, y, val)) for x, y in z])

            yy = idb[len(z):]
            if yy:
                rb[repb] = yy
            else:
                del rb[repb]
    else:
        for k, v in ra.items():
            val = ia[k]
            result.extend([flatten(("delete", i, na_val, val)) for i in v])

    del ra, ia

    for k, v in rb.items():
        val = ib[k]
        result.extend([flatten(("insert", na_val, i, val)) for i in v])

    del rb, ib

    if result:
        if sort:
            def indexsort(x):
                i, j = x[1:3]
                return ([i, j][i == na_val], [j, 0][j == na_val])
            result.sort(key=indexsort, reverse=reverse)

        if header:
            maxcol = max(map(len, result)) - 3
            head = [["tag", "index_a", "index_b", *map("col_{:02d}".format, range(maxcol))]]
            result = head + result
    return result

def to_excel(rows, outputfile, sheetname="Sheet1",
            header = True, startrow=0, startcol=0, conditional_value=" ---> "):
    import xlsxwriter

    i = startrow
    j = startcol

    with xlsxwriter.Workbook(outputfile) as wb:
        ws = wb.add_worksheet(sheetname)
        write = ws.write_row

        if header:
            if hasattr(rows, "__next__"):
                row = next(rows)
            else:
                row, *rows = rows
            props_h = dict(border=1, bold=True, align="center", valign="center")
            write(i, startcol, row, wb.add_format(props_h))
            i += 1

        border = wb.add_format(dict(border=1, valign="center"))

        for row in rows:
            write(i, startcol, row, border)
            icol = startcol + len(row) - 1
            if j < icol:
                j = icol
            i += 1

        ws.autofilter(startrow, startcol, i, j)

        redfm = wb.add_format(
            dict(bg_color='#FFC7CE',
                font_color='#9C0006')
        )

        ws.conditional_format(startrow, startcol, i, j,
            dict(type='text',
                criteria='containing',
                value=conditional_value,
                format=redfm)
        )


def selector(key, start=0, tar="target"):
    if not key:
        return
    if isinstance(key, (list, tuple, range)):
        ret = key
    elif re.search("[^0-9,\-]", key):
        ret = key.split(",")
    else:
        ret = []
        for x in key.split(","):
            if "-" in x:
                s,e = x.split("-")
                ret.extend([i + (start * -1) for i in range(int(s),int(e)+1)])
            else:
                ret.append(int(x) + (start * -1))

    try:
        num = list(map(int, ret))
        it = itemgetter(*num)
        if tar:
            def getter(x):
                if hasattr(x, "__next__"):
                    x = list(x)
                if len(num) == 1 or len(x) == 1:
                    return [it(x)]
                return it(x)
            return getter
        else:
            def getter(x):
                return map(it, x)
            return getter

    except ValueError:
        if tar:
            def getter(x):
                if hasattr(x, "__next__"):
                    x = list(x)
                dic = [getattr(xx, tar) for xx in x]
                return (x[dic.index(r)] for r in ret)
        else:
            def getter(x):
                if hasattr(x, "__next__"):
                    x = list(x)
                it = itemgetter(*map(x[0].index, ret))
                return map(it, x)
        return  getter

def main():
    import os
    from argparse import ArgumentParser
    from operator import attrgetter
    from util.io import readrow, grouprow, to_csv, to_tsv, unicode_escape
    from util.filetype import guesstype

    ps = ArgumentParser(prog="differ",
                        description="2 file diff compare program\n")
    padd = ps.add_argument

    padd('-V','--version', action='version', version='%(prog)s ' + __version__)
    padd('file1', nargs=1, help='diff before file')
    padd('file2', nargs=1, help='diff after file')

    padd('-a', '--all', action='store_true', default=False,
         help='All Line print(default False)')
    padd('-o', '--outfile', type=str, default=None,
         help='output filepath (default `stdout`)')
    padd('-e', '--encoding', type=str, default="cp932",
         help='output fileencoding (default `cp932`)')
    padd('-r', '--rate', type=float, default=0.6,
         help='matched score rate (default `0.6`)')
    padd('-s', '--sep', type=unicode_escape, default="\t",
         help='output separater (default `\\t`)')
    padd('-l', '--lineterminator', type=unicode_escape, default="\r\n",
         help='output llineterminator (default `\\r\\n`)')
    padd('-n', '--noheader', action="store_true", default=False,
         help='file no header (default `False`)')
    padd('-N', '--navalue', type=str, default="-",
         help='output index N/A value (default `-`)')
    padd('-C', '--condition_value', type=str, default=" ---> ",
         help='Delimiter String of Replace Value (default ` ---> `)')

    padd('-t', '--target', type=selector, default=None,
         help='target table names or sheetname (ex. Sheet1, Sheet3)')
    padd('-t1', '--target1', type=selector, default=None,
         help='target table names or sheetname of filename1 (ex. Sheet1, Sheet3)')
    padd('-t2', '--target2', type=selector, default=None,
         help='target table names or sheetname of filename2 (ex. Sheet1, Sheet3)')

    args = ps.parse_args()

    p1 = args.file1[0]
    p2 = args.file2[0]

    na_value = args.navalue
    diffonly = not args.all
    outputfile = args.outfile
    header = not args.noheader
    encoding = args.encoding
    sep = args.sep
    lineterminator = args.lineterminator
    rep_rate = args.rate

    conditional_value = args.condition_value

    notarget = ["ppt","doc","csv","txt","html","pickle"]
    if guesstype(p1) not in notarget and guesstype(p2) not in notarget:

        #TODO very big data dictionary memory NG
        tar1select = args.target1 or args.target
        tar2select = args.target2 or args.target
        a = {x.target:x.value for x in (tar1select(grouprow(p1)) if tar1select else grouprow(p1))}
        b = {x.target:x.value for x in (tar2select(grouprow(p2)) if tar2select else grouprow(p2))}

        #similar target
        kw = dict(
            header=header,
            diffonly=diffonly,
            rep_rate=rep_rate,
            na_val=na_value,
            startidx=1,
            conditional_value=conditional_value)

        def _simtar(tag, tar, a, b, i):
            kw["header"] = i==0 and kw["header"]

            if tag == "equal":
                row = ([tar,*r] for r in differ(a[tar], b[tar], **kw))
            elif tag == "replace":
                ta, tb = tar.split(conditional_value)
                row = ([tar,*r] for r in differ(a[ta], b[tb], **kw))
            elif tag == "delete":
                row = ([tar, tag, j, na_value,*r] for j, r in enumerate(a[tar], 1))
            elif tag == "insert":
                row = ([tar, tag, na_value, j, *r] for j, r in enumerate(b[tar], 1))
            else:
                return []

            return row

        it = (["targetname", *r[1:]] if r[1] == "tag" else r
                  for i, (tag, *_, tar) in enumerate(differ(a, b))
                      for r in _simtar(tag, tar, a, b, i))

    else:
        a = map(attrgetter("value"), readrow(p1))
        b = map(attrgetter("value"), readrow(p2))

        it = differ(
            a, b,
            header=header,
            diffonly=diffonly,
            rep_rate=rep_rate,
            na_val=na_value,
            startidx=1,
            conditional_value=conditional_value
        )

    if outputfile is None:
        return to_csv(it, sys.stdout, encoding=encoding, sep=sep)

    ext = os.path.splitext(outputfile)[-1].startswith

    if ext(".xls"):
        to_excel(it, outputfile, header=True, conditional_value=conditional_value)
    elif ext(".tsv"):
        to_tsv(it, outputfile, encoding=encoding, lineterminator=lineterminator)
    else:
        to_csv(it, outputfile, encoding=encoding, lineterminator=lineterminator)

def test():
    from util.core import tdir
    from util.io import readrow

    from datetime import datetime as dt
    from io import StringIO


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
        assert(sanitize(list("abc"),list("abb")) == ['a', 'b', 'c ---> b'])
        assert(sanitize(["abc"],["abb"]) == ['abc ---> abb'])

    def test_deephash():
        assert(deephash("a") == (hash("a"),))
        assert(deephash(["0",["1","2"],[["3", "4"]]]) == (hash("0"),(hash("1"),hash("2")),((hash("3"), hash("4")),)))

    def test_similar():
        assert(1 > similar(deephash("abc"), deephash("abb")) > 0.6)
        assert(similar(deephash(("abc",)), deephash(("abb",))) == 0.0)

    def test_differ1d():
        a = "abc"
        b = "bcd"
        assert(list(differ(a, b)) == [('delete', 0, None, 'a'), ('equal', 1, 0, 'b'), ('equal', 2, 1, 'c'), ('insert', None, 2, 'd')])
        assert(differ(a, b, startidx=1) == [('delete', 1, None, 'a'), ('equal', 2, 1, 'b'), ('equal', 3, 2, 'c'), ('insert', None, 3, 'd')])
        a = list(range(1,3))
        b = list(range(2, 4))
        assert(list(differ(a, b)) == [('delete', 0, None, 1), ('equal', 1, 0, 2), ('insert', None, 1, 3)])

    def test_differ2d():
        a = [list("abc"), list("abc")]
        b = [list("abc"),list("acc"), list("xtz")]
        assert(list(differ(a, b)) == [('equal', 0, 0, 'a', 'b', 'c'), ('replace', 1, 1, 'a', 'b ---> c', 'c'), ('insert', None, 2, 'x', 't', 'z')])
        assert(differ(a, b, startidx=1) == [('equal', 1, 1, 'a', 'b', 'c'), ('replace', 2, 2, 'a', 'b ---> c', 'c'), ('insert', None, 3, 'x', 't', 'z')])

    def test_differcsv():
        a = (x.value for x in readrow(tdir+"diff1.csv"))
        b = (x.value for x in readrow(tdir+"diff2.csv"))
        assert([x for x in differ(a,b) if x[0]!="equal"] == [('replace', 1, 1, 'b ---> 10', '8', '307', '130', '3504', '12', '70', '1', 'chevrolet chevelle malibue'), ('insert', None, 15, '22', '6', '198', '95', '2833', '15.5', '70', '1', 'plymouth duster'), ('insert', None, 23, '26', '4', '121', '113', '2234', '12.5', '70', '2', 'bmw 2002'), ('replace', 37, 39, '14', '9 ---> 8', '351', '153', '4154', '13.5', '71', '1', 'ford galaxie 500'), ('replace', 46, 48, '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2001 ---> mercury capri 2000'), ('delete', 55, None, '25', '4', '97.5', '80', '2126', '17', '72', '1', 'dodge colt hardtop')])

    def test_selector():
        dat = [list("abc"), list("edf")]
        test1 = list(selector("1,0",tar=None)(dat))
        assert(test1 == [('b', 'a'), ('d', 'e')])
        test2 = list(selector("1,0")(dat))
        assert(test2 == [dat[1], dat[0]])
        test3 = list(selector("b,a", tar=None)(dat))
        assert(test3 == [('b', 'a'), ('d', 'e')])
        from collections import namedtuple
        t = namedtuple("d", ["path", "target", "value"])
        dat = [t("aam", "a", 1), t("aam", "b", 2), t("aam", "c", 3)]
        test4 = list(selector("b,a")(dat))
        assert(test4 == [dat[1], dat[0]])

    def test_benchmark():
        f1 = tdir+"diff2.xlsx"
        f2 = tdir+"diff3.xlsx"
        a = readrow(f1)
        b = readrow(f2)

        t = dt.now()
        differ(a, b, sort=False)
        print("test_nosort_differ: time {}".format(dt.now() - t))

    def stdoutcapture(args):
        sio = StringIO()
        sys.stdout = sio
        del sys.argv[1:]
        sys.argv.extend(args)
        main()
        sio.seek(0)
        sys.stdout = sys.__stdout__
        return sio

    def test_default_main():
        from collections import Counter as cc
        sio = stdoutcapture(["-n", tdir+"diff1.xlsx", tdir+"diff2.xlsx"])
        assert(cc(x.replace('"', '').split("\t")[1] for x in sio) == cc({"replace": 3, "insert": 2, "delete": 1}))

    def test_main_encoding():
        pass

    def test_main_header():
        pass

    def test_main_navalue():
        pass

    def test_main_sheetname_ignore():
        pass

    def test_main_conditon_value():
        pass

    def test_main_all():
        pass

    def test_main_outfile_xls():
        pass

    def test_main_outfile_csv():
        pass

    def test_main_outfile_tsv():
        pass

    def test_main_outfile_txt():
        pass

    def test_target():
        sio = stdoutcapture("-t 0".split(" ") + [tdir+"diff1.xlsx", tdir+"diff2.xlsx"])
        assert(sio.getvalue().count("\n") == 7)

    def test_target1_2():
        sio = stdoutcapture("-t1 0 -t2 0".split(" ") + [tdir+"diff1.xlsx", tdir+"diff2.xlsx"])
        assert(sio.getvalue().count("\n") == 7)
        sio = stdoutcapture("-t1 0 -t2 1".split(" ") + [tdir+"diff2.xlsx", tdir+"diff3.xlsx"])
        assert(sio.getvalue().count("\n") == 7)

    def test_target_sheetname():
        try:
            stdoutcapture("-t diff1".split(" ") + [tdir+"diff1.xlsx", tdir+"diff2.xlsx"])
            raise(AssertionError)
        except ValueError:
            pass

    def test_target1_target2():
        sio = stdoutcapture("-t1 diff1 -t2 diff2".split(" ") + [tdir+"diff1.xlsx", tdir+"diff2.xlsx"])
        assert(sio.getvalue().count("\n") == 7)

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = dt.now()
            func()
            t2 = dt.now()
            print("{} : time {}".format(x, t2-t1),file=sys.stderr)

if __name__ == "__main__":
    # test()
    # sys.argv.extend("C:/temp/diff1.xlsx C:/temp/diff2.xlsx".split(" "))
    main()
