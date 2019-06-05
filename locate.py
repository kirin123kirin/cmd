#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from glob import glob
import sys, os, re

import plocate.mlocatedb

if os.name == "posix":
    mlocatedb = plocate.mlocatedb.mlocatedb
else:
    class mlocatedb(plocate.mlocatedb.mlocatedb):
        def files(self):
            dh = self.next_dirheader()
            while dh is not None:
                fe = self.next_fileentry(dh.dirpath)
                if fe.is_endmark():
                    dh = self.next_dirheader()
                else:
                    fe.filename = fe.filename.replace("\\", "/")
                    yield fe

def locate(f, limit = None):
    with open(f, "rb") as database:
        mdb = mlocatedb(database)
        mdbfiles = mdb.files()
        if limit:
            for _ in range(limit):
                yield next(mdbfiles)
        else:
            for m in mdbfiles:
                yield m

def dumper(database, pattern=None, outtype="file", limit=None, hostname=None):
    from collections import defaultdict

    hostname = hostname or os.path.basename(database).replace(".db", "")

    ld = locate(database, limit)

    pt = re.compile(pattern).search if pattern else lambda p: True

    if outtype == "dir":
        for x in ld:
            if x.is_dir() and pt(x):
                yield [hostname, x, -1, *map("/".__add__, x[1:].split("/"))]

    elif outtype == "file":
        ret = defaultdict(int)
        for o in ld:
            path = o.filename.rsplit("/", 1)[0] or "/" # dirname

            if o.is_file() and pt(path):
                if ret and path not in ret:
                    for k,v in ret.items():
                        dirs = map("/".__add__, k[1:].split("/"))
                        yield [hostname, k, v, *dirs]
                    ret.clear()

                ret[path] += 1

    else:
        for o in ld:
            x = o.filename
            if pt(x):
                yield [hostname, x, 1, *map("/".__add__, x[1:].split("/"))]

def eachlocate(files, header=None, pattern=None, outtype="file", limit=None):
    if header:
        yield ["server", "fullpath", "count", "DIRS*"]
    for f in files:
        for d in  dumper(f, pattern, outtype, limit):
            yield d

def to_csv(iterator, file=sys.stdout, **kw):
    import csv
    fp = file if hasattr(file, "write") else open(file, "w")
    if fp == sys.stdout:
        kw["lineterminator"] = "\n"
    writer = csv.writer(fp, **kw)
    for x in iterator:
        writer.writerow(x)
        fp.flush()
    if not hasattr(file, "write"):
        fp.close()

def to_excel(iterator, file):
    from xlsxwriter import Workbook
    with Workbook(file) as book:
        header_fmt = book.add_format(
            dict(border=True, align="center", bold=True))

        sheet = book.add_worksheet()

        for i, row in enumerate(iterator, 1):
            sheet.write_row(i, 0, row)

        dirs = map("DIR{:02d}".format, range(sheet.dim_colmax - 2))
        sheet.write_row(0, 0, ["server", "fullpath", "count", *dirs], header_fmt)
        sheet.autofilter(0, 0, sheet.dim_rowmax, sheet.dim_colmax)

def main():
    from argparse import ArgumentParser
    usage="""
    path string dump from mlocate.db
       Example1: {0} *.db -o fileslist.csv
       Example2: {0} *.db -o fileslist.xlsx

    """.format(os.path.basename(sys.argv[0]).replace(".py", ""))

    ps = ArgumentParser(usage)
    padd = ps.add_argument

    padd('-o', '--outfile', type=str, default=None,
         help='output filepath (default `stdout`)')
    padd("-l", "--limit",
         help="db dump limit lines number",
         type=int, default=None)
    padd('-t', '--type', type=str, default="file",
         help='mlocate.db dumptype. `file` or `dir` or `all`')
    padd('-r', '--regex', type=str, default=None,
         help='regex path filter string')
    padd("-H", "--header",
         help="header print",
         action='store_true', default=False)
    padd("filename",
         metavar="<filename>",
         nargs="+",  default=[],
         help="target mlocate.db files")
    args = ps.parse_args()

    header = args.header
    outtype = None if args.type == "all" else args.type
    limit = args.limit
    pattern = args.regex and args.regex.strip("'\"")
    outfile = args.outfile or sys.stdout

    files = [g for fn in args.filename for g in glob(fn)]
    if not files:
        raise RuntimeError("Not found files {}".format(args.filename))

    it = eachlocate(files, header, pattern, outtype, limit=limit)

    if str(outfile).lower().rsplit(".", 1)[1].startswith("xls"):
        to_excel(it, outfile)
    else:
        to_csv(it, outfile)


if __name__ == "__main__":
    #sys.argv.extend('C:/temp/mlocate.db -o C:/temp/test.xlsx'.split(" "))
    #sys.argv.append("mlocate.db")

    main()

