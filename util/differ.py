#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""

__version__ = "0.3.0"
__author__ = "m.yama"


__all__ = ["differ"]


from itertools import zip_longest

try:
    from similar import similar

    from functools import  _CacheInfo, _lru_cache_wrapper
    similar = _lru_cache_wrapper(similar, 128, False, _CacheInfo)
except:
    from collections import _count_elements as Counter

    from functools import  lru_cache
    @lru_cache()
    def similar(a:tuple, b:tuple):
        """
            Parameters:
                a: tuple (Compare target data left)
                b: tuole (Compare target data right)
            Return:
                float (0.0 < return <= 1.000000000002)
        """
        ca = {}
        Counter(ca, a)
        cb = {}
        cab = {}

        for k in b:
            if k in cb:
                cb[k] += 1
            else:
                cb[k] = 1

            if k in ca:
                cab[k] = ca[k] + cb[k]

        if cab:
            agg = len(a) + len(b)
            return sum(cab.values()) / agg
        return 0.0


try:
    from xsorted import xsorted # awesome
except ModuleNotFoundError:
    xsorted = sorted

BASE_TYPE = [int, float, str, bytes, bytearray, bool]

def flatten(*x):
    return [z for y in x for z in ([y] if y is None or type(y) in BASE_TYPE else flatten(*y))]

#@lru_cache(maxsize=16)
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
    if a is None or type(a) in BASE_TYPE and b is None or type(b) in BASE_TYPE:
        return compare(a, b, **kw)
    else:
        return [compare(x, y, **kw) for x, y in zip_longest(a, b, fillvalue="")]

def deephash(x):
    if type(x) in [int, float, bool]:
        return hash(x)
    return tuple(hash(y) if type(y) in BASE_TYPE else deephash(y) for y in x)

def hashidx(x):
    ret = {}
    if not x:
        return {}
    if type(x) in [int, float]:
        return {0: x}
    if isinstance(x, dict):
        x = x.items()

    for i, v in enumerate(x):
        h = deephash(v)
        if h in ret:
            ret[h].append(i)
        else:
            ret[h] = [i]
    return ret


def maxsimilar(repa, ia, ib, rep_rate=0.6):
    rate, repbkey, repidx = 0.0, None, None
    for repb in ib:
        if repb:
            rt = similar(repa, repb)
            if rep_rate < rt and rate < rt:
                rate = rt
                repbkey = repb
                repidx = zip(ia[repa], ib[repb])
    return rate, repbkey, repidx

def forcelist(x):
    if hasattr(x, "__next__"):
        return list(x)
    elif hasattr(x, "itertuples"):
        return list(x.itertuples(index=False))
    elif hasattr(x, "readlines"):
        return x.readlines()
    else:
        return x

def idiff(a, b, notequal=False, rep_rate=0.6, na_val=None, **kw):
    a = forcelist(a)
    b = forcelist(b)

    ia = hashidx(a)
    ib = hashidx(b)

    # Analyze equal Section
    for k in ia.keys() & ib.keys():
        iak, ibk = ia[k], ib[k]
        for ii, (x, y) in enumerate(zip(iak, ibk), 1):
            if notequal:
                continue
            yield flatten("equal", x, y, a[x])
        iia, iib = iak[ii:], ibk[ii:]

        if iia:
            ia[k] = iia
        else:
            del ia[k]

        if iib:
            ib[k] = iib
        else:
            del ib[k]

    # Analyze replace Section
    if 0.0 < rep_rate < 1.0:
        for repa in ia.copy():
            if repa:
                try:
                    rate, repbkey, repidx = maxsimilar(repa, ia, ib, rep_rate=rep_rate)
                except TypeError:
                    continue

                if rate:# and repbkey and repidx:
                    for m, (ria, rib) in enumerate(repidx, 1):
                        yield flatten("replace", ria, rib, sanitize(a[ria], b[rib], **kw))

                    rma, rmb = ia[repa][m:], ib[repbkey][m:]
                    if rma:
                        ia[repa] = rma
                    else:
                        del ia[repa]

                    if rmb:
                        ib[repbkey] = rmb
                    else:
                        del ib[repbkey]


    # Analyze delete Section
    yield from (flatten("delete", i, na_val, a[i]) for v in ia.values() for i in v)
    del ia

    # Analyze Insert Section
    yield from (flatten("insert", na_val, i, b[i]) for v in ib.values() for i in v)
    del ib


def differ(a, b, notequal=False, sort=True, reverse=False, rep_rate=0.6, na_val=None, **kw):
    d = idiff(a, b, notequal=notequal, rep_rate=rep_rate, na_val=na_val, **kw)
    if sort:
        def indexsort(x):
            i, j = x[1:3]
            return ([i, j][i == na_val], [j, 0][j == na_val])

        return xsorted(d, key=indexsort, reverse=reverse)
    return d


def main():
    import sys
    import os
    from argparse import ArgumentParser
    from util import Path
    from util.dfutil import pd

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
    padd('-n', '--noheader', action="store_true", default=False,
         help='file no header (default `False`)')
    padd('-N', '--navalue', type=str, default="-",
         help='output index N/A value (default `-`)')
    padd('-S', '--sheetnames_ignore', action='store_true', default=False,
         help='Compare with sheet names (default True)')
    padd('-C', '--condition_value', type=str, default=" ---> ",
         help='Delimiter String of Replace Value (default ` ---> `)')

    args = ps.parse_args()

    p1 = Path(args.file1[0])
    p2 = Path(args.file2[0])

    sheetname_compare = not args.sheetnames_ignore
    na_value = args.navalue
    diffonly = not args.all
    header = not args.noheader
    encoding = args.encoding
    conditional_value = args.condition_value

    a = p1.readlines(return_target=sheetname_compare)
    b = p2.readlines(return_target=sheetname_compare)

    it = differ(
        a, b,
        notequal=diffonly,
        na_val=na_value,
        conditional_value=conditional_value
    )

    df = pd.DataFrame(it, dtype=object)

    df.columns = ["tag", "index_a", "index_b", *map("col{:03d}".format, range(len(df.columns)-3))]

    outputfile = Path(args.outfile) if args.outfile else sys.stdout
    if outputfile is sys.stdout:
        return df.to_csv(outputfile, index=False, header=header, encoding="cp932" if os.name == "nt" else "utf8")

    ext = outputfile.ext.startswith
    if ext(".xls"):
        df.to_excel_plus(outputfile, index=False, header=header, conditional_value=conditional_value)
    elif ext(".csv"):
        df.to_csv(outputfile, index=False, header=header, encoding=encoding)
    elif ext(".htm"):
        df.to_html(outputfile, index=False, header=header)
    elif ext(".tsv"):
        df.to_csv(outputfile, sep="\t", index=False, header=header, encoding=encoding)
    elif ext(".json"):
        df.to_json(outputfile, index=False, header=header)
    else:
        df.to_csv(outputfile, index=False, header=header, encoding=encoding)

def test():
    from util.core import tdir
    from util import Path
    from io import StringIO

    from datetime import datetime as dt
    import sys


    def test_flatten():
        assert(flatten([0,[1,2],[[3,4]]]) == [0, 1, 2, 3, 4])
        assert(flatten([]) == [])
        assert(flatten(1) == [1])
        assert(flatten("abc") == ["abc"])

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

    def test_hashidx():
        assert(hashidx("abc") == {(hash("a"),): [0], (hash("b"),): [1], (hash("c"),): [2]})

    def test_similar():
        assert(1 > similar(tuple("abc"), tuple("abb")) > 0.7)
        assert(similar(("abc",), ("abb",)) == 0.0)

    def testidiff1d():
        a = "abc"
        b = "bcd"
        assert(set(map(tuple,idiff(a, b))) == {('equal', 1, 0, 'b'), ('equal', 2, 1, 'c'), ('insert', None, 2, 'd'), ('delete', 0, None, 'a')} )

    def test_differ1d():
        a = "abc"
        b = "bcd"
        assert(list(differ(a, b)) == [['delete', 0, None, 'a'], ['equal', 1, 0, 'b'], ['equal', 2, 1, 'c'], ['insert', None, 2, 'd']])
        a = list(range(1,3))
        b = list(range(2, 4))
        assert(list(differ(a, b)) == [['delete', 0, None, 1], ['equal', 1, 0, 2], ['insert', None, 1, 3]])

    def test_differ2d():
        a = [list("abc"), list("abc")]
        b = [list("abc"),list("acc"), list("xtz")]
        assert(list(differ(a, b)) == [['equal', 0, 0, 'a', 'b', 'c'], ['replace', 1, 1, 'a', 'b ---> c', 'c'], ['insert', None, 2, 'x', 't', 'z']])

    def test_differcsv():
        a = Path(tdir+"diff1.csv").readlines()
        b = Path(tdir+"diff2.csv").readlines()
        assert([x for x in differ(a,b) if x[0]!="equal"] == [['replace', 1, 1, 'b ---> 10', '8', '307', '130', '3504', '12', '70', '1', 'chevrolet chevelle malibue'], ['insert', None, 15, '22', '6', '198', '95', '2833', '15.5', '70', '1', 'plymouth duster'], ['insert', None, 23, '26', '4', '121', '113', '2234', '12.5', '70', '2', 'bmw 2002'], ['replace', 37, 39, '14', '9 ---> 8', '351', '153', '4154', '13.5', '71', '1', 'ford galaxie 500'], ['replace', 46, 48, '23', '4', '122', '86', '2220', '14', '71', '1', 'mercury capri 2001 ---> mercury capri 2000'], ['delete', 55, None, '25', '4', '97.5', '80', '2126', '17', '72', '1', 'dodge colt hardtop']])

    def stdoutcapture(*args):
        sio = StringIO()
        sys.stdout = sio
        del sys.argv[1:]
        sys.argv.extend(args)
        main()
        sio.seek(0)
        sys.stdout = sys.__stdout__
        return sio

    def _test_default_main():
        from collections import Counter as cc
        sio = stdoutcapture("-n", tdir+"diff1.csv", tdir+"diff2.csv")
        assert(cc(x.split(",")[0] for x in sio) == cc({"replace": 3, "insert": 2, "delete": 1}))

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

    def test_main_outfile_html():
        pass

    def test_main_outfile_json():
        pass

    def test_main_outfile_tsv():
        pass

    def test_main_outfile_txt():
        pass

    def test_benchmark():
        f1 = tdir+"diff2.xlsx"
        f2 = tdir+"diff3.xlsx"
        a = Path(f1).readlines()
        b = Path(f2).readlines()

        t = dt.now()
        list(differ(a, b, sort=False))
        print("test_nosort_differ: time {}".format(dt.now() - t))

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = dt.now()
            func()
            t2 = dt.now()
            print("{} : time {}".format(x, t2-t1),file=sys.stderr)



if __name__ == "__main__":
#    test()
    main()
