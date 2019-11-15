#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""
__version__ = "0.3.0"
__author__ = "m.yama"


__all__ = [
        "profiler",
        "profile_data",
]

from itertools import  zip_longest
from  collections import  _count_elements
import heapq
from collections import namedtuple
import sys
import csv
import os
from itertools import combinations
from pathlib import Path

from util.io import readrow, grouprow
from util.filetype import guesstype

BASE_TYPE = [type(None), int, float, str, bytes, bytearray, bool]
def deeptuple(x):
    try:
        return tuple([y if type(y) in BASE_TYPE else deeptuple(y) for y in x])
    except:
        return (x, )

ret = namedtuple("Profile",
    ["rec_count", "is_notnull", "is_uniq", "notnull_count", "null_count", "uniq_count", "fill_rate", "uniq_rate", "key_rate", "top"]
)


def guess_key(profile, n=10):
    keys = sorted([(v.key_rate, k) for k, v in profile.items()])[-19:]
    result = []
    for i in range(1, len(keys)):
        for x in combinations(keys, i):
            rk = []
            rv = 0
            for v, k in x:
                rk.append(k)
                rv += v ** v
            result.append((rv**0.5, rk))
    return [k for v, k in sorted(result, reverse=True)[:n]]

def profile_data(rows, header=None, top=10, na_val=[None, "", "N/A", "NULL", "null", "none", "na"]):
    result = {}

    rec = None
    col = []

    if isinstance(header, int):
        if hasattr(rows, "__next__"):
            for i in range(header + 1):
                col = next(rows)
        else:
            col, *rows = rows[header:]
    elif isinstance(header, (list, tuple)):
        col = header

    for i, x in enumerate(zip_longest(*rows)):
        if rec is None:
            rec = len(x)

        counter = {}
        gc = counter.get

        _count_elements(counter, [y for y in x if y not in na_val])

        uq_n = len(counter)
        uq_rate = uq_n / rec

        notna_n = sum(counter.values())

        topN = heapq.nlargest(top, counter, key=gc) if top > 0 else []

        del counter, gc

        na_n = rec - notna_n
        fill_rate = notna_n / rec
        if uq_rate > 0 and fill_rate > 0:
            key_rate = (uq_rate * fill_rate) / ((pow(uq_rate, 2) + pow(fill_rate, 2)) ** 0.5)
        else:
            key_rate = 0.0

        info = ret(
            rec,
            rec == notna_n,
            rec == uq_n,
            notna_n,
            na_n,
            uq_n,
            fill_rate,
            uq_rate,
            key_rate,
            topN
        )

        k = col[i] if i < len(col) else i

        result[k] = info

    return result

def profiler(
    path_or_buffer,
    header=None,
    top=10,
    na_val = [None, "", "N/A", "NULL", "null","none", "na"],
    headerout=True,
    ):

    kw = dict(
        header=header,
        top=top,
        na_val=na_val,
    )

    head = []

    try:
        if guesstype(path_or_buffer) in ["ppt","doc","csv","txt","html","pickle"]:
            raise ValueError

        rows = grouprow(path_or_buffer)

        if headerout:
            head = [["targetname", "columns", *ret._fields]]
        return head + [[pk, ck, *cv] for _, pk, row in rows for ck, cv in profile_data(row, **kw).items()]
    except ValueError:
        rows = (x.value for x in readrow(path_or_buffer))
        if headerout:
            head = [["columns", *ret._fields]]
        return head + [[k, *v] for k, v in profile_data(rows, **kw).items()]


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

defaultencoding = "cp932" if os.name == "nt" else "utf-8"

def to_csv(rows, outputfile, encoding, lineterminator="\r\n", **kw):
    with open(outputfile, "w", encoding=encoding, newline="") as f:
        writer = csv.writer(f, lineterminator=lineterminator, **kw)
        writer.writerows(rows)

def to_tsv(rows, outputfile, encoding, lineterminator="\r\n", **kw):
    return to_csv(rows, outputfile, encoding, lineterminator, delimiter="\t", quoting=csv.QUOTE_NONE, **kw)


def main():
    from glob import glob
    from argparse import ArgumentParser

    ps = ArgumentParser(prog="differ",
                        description="data profile program\n")
    padd = ps.add_argument

    padd('-V','--version', action='version', version='%(prog)s ' + __version__)
    padd("-v", "--verbose", help="print progress",
         action='store_true', default=False)
    padd("filename",
         metavar="<filename>",
         nargs="+",  default=[],
         help="target any files(.txt, .csv, .tsv, .xls[x],)")

    padd('-o', '--outfile', type=str, default=None,
         help='output filepath (default `stdout`)')
    padd('-e', '--encoding', type=str, default="cp932",
         help='output fileencoding (default `cp932`)')
    padd('-l', '--lineterminator', type=str, default="\r\n",
         help='output fileencoding (default `\\r\\n`)')
    padd('-H', '--header', type=int, default=0,
         help='number of header Input File index. (start is 0) (default 0)')
    padd('-N', '--navalue', type=str, default=[],
         help='output index N/A value (default `-`)')
    padd('-t', '--top', type=int, default=10,
         help='frequency values top count (default `10`)')
    args = ps.parse_args()

    def walk(args):
        for arg in args.filename:
            for f in glob(arg):
                f = os.path.normpath(f)
                if args.verbose:
                    sys.stderr.write("Profiling:{}\n".format(f))
                    sys.stderr.flush()
                yield f

    na_value = [None, "", "N/A", "NULL", "null","none", "na"]
    na_value.extend(args.navalue.split(",") if args.navalue else args.navalue)
    header = args.header
    encoding = args.encoding
    lineterminator = args.lineterminator
    top = args.top

    def it():
        i = None
        for i, f in enumerate(walk(args)):
            j = 0
            for x in profiler(
                f,
                header=header,
                top=top,
                na_val=na_value,
                headerout= i == 0,
                ):
                if i + j == 0:
                    yield ["filename"] + x
                else:
                    yield [f] + x
                j += 1
        if i is None:
            raise FileNotFoundError(str(args.filename))

    if args.outfile:
        outputfile = Path(args.outfile)
    else:
        writer = csv.writer(sys.stdout, lineterminator="\n")
        return writer.writerows(it())

    ext = outputfile.suffix.startswith
    if ext(".xls"):
        to_excel(it(), outputfile, header=header, conditional_value="False")
    elif ext(".tsv"):
        to_tsv(it(), outputfile, encoding=encoding, lineterminator=lineterminator)
    else:
        to_csv(it(), outputfile, encoding=encoding, lineterminator=lineterminator)


def test():
    from datetime import datetime as dt
    from util.core import tdir
    from io import StringIO

    def test_profile_data():
        a = [list("ABCD")] + [list("abcd")] * 3
        r = profile_data(a)[0]
        assert(r.rec_count == 4)
        assert(r.is_notnull is True)
        assert(r.is_uniq is False)
        assert(r.top == list("aA"))

    def test_profile_data_uniq():
        a = zip(*[list(range(10))])
        r = profile_data(a)[0]
        assert(r.rec_count == 10)
        assert(r.is_notnull is True)
        assert(r.is_uniq is True)
        assert(r.top == list(range(10)))

    def test_profile_data_header_0():
        a = [list("ABCD")] + [list("abcd")] * 3
        r = profile_data(a, header=0)["A"]
        assert(r.rec_count == 3)
        assert(r.is_notnull is True)
        assert(r.is_uniq is False)
        assert(r.top == list("a"))

    def test_profile_data_header_n():
        a = [list("ABCD")] + [list("abcd")] * 3
        r = profile_data(a, header=2)["a"]
        assert(r.rec_count == 1)
        assert(r.is_notnull is True)
        assert(r.is_uniq is True)
        assert(r.top == list("a"))

    def test_profile_data_topn():
        a = [list("ABCD")] + [list("abcd")] * 3
        r = profile_data(a, top=1)[0]
        assert(len(r.top) == 1)
        r = profile_data(a, top=0)[0]
        assert(r.top == [])

    def test_profile_data_naval():
        a = [list("AB")] + [[None] * 2, [""] * 2, ["N/A", "hoge"]]
        rr = profile_data(a, header=0)
        r = rr["A"]
        assert(r.rec_count == 3)
        assert(r.null_count == 3)
        assert(rr["B"].null_count == 2)
        assert(rr["B"].notnull_count == 1)

    def test_guess_key():
        a = profile_data((x.value for x in readrow(tdir+"diff1.xlsx")), header=0)
        assert(len(guess_key(a, 10)) == 10)

    def test_profiler():
        r = profiler(tdir+"test.csv")
        assert(len(r) == 3)
        assert(r[0][0] == "columns")
        assert(len(r[0]) == len(r[1]))
        assert(r[1][1] == 3)

    def test_benchmark():
        profiler(tdir+"diff1.csv")

    def stdoutcapture(args):
        sio = StringIO()
        sys.stdout = sio
        del sys.argv[1:]
        sys.argv.extend(args)
        main()
        sio.seek(0)
        sys.stdout = sys.__stdout__
        return sio

    def test_main():
        sio = stdoutcapture([tdir+"diff1.xlsx"])
        assert(sio.getvalue().count("\n") == 10)

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = dt.now()
            func()
            t2 = dt.now()
            print("{} : time {}".format(x, t2-t1))

if __name__ == "__main__":
#    test()
    main()
