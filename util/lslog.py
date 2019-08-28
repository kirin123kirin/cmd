#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__version__ = "0.3.0"
__author__ = "m.yama"


__all__ = [
        "parse_lslR",
        "parse_lsl"
    ]

import re, sys
from datetime import datetime
from posixpath import basename, normpath, join as pathjoin

from util.utils import to_datetime
from util.core import opener


def split_ls(line, parent=None, base_date=None, mod_sp = re.compile(" +")):
    if line.startswith("l"):
        p, _, o, g, size, *d, name, _, ln = mod_sp.split(line)
    else:
        ln = ""
        p, _, o, g, size, *d, name = mod_sp.split(line)

    dt = to_datetime(" ".join(d))

    if base_date is None:
        base_date = datetime.now()
    elif isinstance(base_date, str):
        base_date = to_datetime(base_date)

    if dt > base_date:
        dt = dt.replace(year=base_date.year - 1)

    dt = dt.strftime("%Y/%m/%d"), dt.strftime("%H:%M:%S")

    try:
        size = int(size)
    except ValueError:
        pass
    
    ret = [p, o, g, size, *dt, name.split(".")[-1] if "." in name else "", name]

    if not parent or parent == ".":
        return [*ret, ln]

    parent = normpath(parent)
    fullpath = pathjoin(parent, name)
    diritem = (fullpath if p.startswith("d") else parent).strip("/").split("/")

    return [*ret, ln, fullpath, *map("/".__add__, diritem)]


def parse_lslR(path_or_buffer, parent=None, base_date=None):
    with opener(path_or_buffer) as fp:
        for x in fp:
            line = x.rstrip()
            if line == "":
                continue
            
            if line.endswith(":"):
                parent = line.rstrip(":")
            
            if parent and line[0] in "-dlcb":
                yield split_ls(line, parent)

def parse_lsl(path_or_buffer, parent=None, base_date=None):
    with opener(path_or_buffer) as fp:
        for x in fp:
            line = x.rstrip()
            if line and line[0] in "-dlcb":
                yield split_ls(line, parent=parent, base_date=base_date)

def to_csv(
    iteratable,
    outfile,
    sep=',',
    header=["permission", "owner", "group", "size", "date", "time", "ext", "basename", "link", "fullpath", "DIR*"],
    quotechar='"',
    quoting=0,
    **kw
    ):

    import csv
    fp = outfile if hasattr(outfile, "write") else open(outfile, "w")
    kw["lineterminator"] = "\n"
    writer = csv.writer(fp, delimiter=sep, quotechar=quotechar, quoting=quoting, **kw)
    if header:
        writer.writerow(header)
    for x in iteratable:
        writer.writerow(x)
        fp.flush()

def to_excel(
    iteratable,
    outfile,
    header=["permission", "owner", "group", "size", "date", "time", "ext", "basename", "link", "fullpath", "DIR*"]
    ):

    from xlsxwriter import Workbook

    with Workbook(outfile) as book:
        header_fmt = book.add_format(
            dict(border=True, align="center", bold=True))

        sheet = book.add_worksheet()
        if header:
            sheet.write_row(0, 0, header, header_fmt)

        for i, row in enumerate(iteratable, 1):
            sheet.write_row(i, 0, row)

        if not sheet.dim_colmax:
            sys.stderr.write("No Data\n")
            return

        if header:
            sheet.autofilter(0, 0, sheet.dim_rowmax, sheet.dim_colmax)

def main():
    from argparse import ArgumentParser
    from glob import  glob

    usage="""
    parse from `ls -lR` log string
       Example1: {0} *.log -o fileslist.csv
       Example2: {0} *.log -o fileslist.xlsx

    """.format(basename(sys.argv[0]).replace(".py", ""))

    ps = ArgumentParser(usage)
    padd = ps.add_argument

    padd('-o', '--outfile', type=str, default=sys.stdout,
         help='output filepath (default `stdout`)')
    padd('-b', '--basedate', type=str, default=None,
         help='run ls -lR Date')
    padd('-c', '--currentdirectory', type=str, default=None,
         help='run ls -lR Current Directory Path String')
    padd('-s', '--sep', type=str, default=",",
         help='csv output separator (default `,`)')

    padd("filename",
         metavar="<filename>",
         nargs="+",  default=[],
         help="target mlocate.db files")
    args = ps.parse_args()

    outfile = args.outfile
    sep = args.sep
    BASE_DATE = args.basedate
    PARENT = args.currentdirectory

    files = [g for fn in args.filename for g in glob(fn)]
    if not files:
        raise RuntimeError("Not found files {}".format(args.filename))

    rows = (row for f in files for row in parse_lslR(f, parent=PARENT, base_date=BASE_DATE))

    ext = str(outfile).lower().rsplit(".", 1)[1]

    if ext.startswith("tsv"):
        to_csv(rows, outfile, sep="\t")

    elif ext.startswith("xls"):
        to_excel(rows, outfile)

    else:
        to_csv(rows, outfile, sep=sep)

def test(path):
    
    def test_parse_lslR():
        assert len(list(parse_lslR(path))) == 7

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = datetime.now()
            func()
            t2 = datetime.now()
            print("{} : time {}".format(x, t2-t1))
     
if __name__ == "__main__":
#    test("C:/temp/lsdir.log")
    main()

