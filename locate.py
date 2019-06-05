#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from glob import glob
import sys, os

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

def locate(f, outtype = "dir", limit = None):
    with open(f,"rb") as database:
        mdb = mlocatedb(database)
        try:
            o = {"dir": 1, "file": 0}[outtype]
        except KeyError:
            raise ValueError("Unknown outtype. {}".format(outtype))

        if limit:
            i = 0
            for m in mdb.files():
                if m.kind == o:
                    yield m.filename
                    i += 1
                if i > limit:
                    break
        else:
            for m in mdb.files():
                if m.kind == o:
                    yield m.filename

def dumper(database, outtype="file", limit=None, hostname=None):
    from collections import defaultdict

    hostname = hostname or os.path.basename(database).replace(".db", "")

    ld = locate(database, outtype, limit)

    if outtype == "dir":
        for x in ld:
            yield [hostname, x, -1, *map("/".__add__, x[1:].split("/"))]
    else:
        ret = defaultdict(int)
        cnt = 0
        for x in ld:
            n = x.count("/")
            if n == cnt:
                ret[x.rsplit("/", 1)[0] or "/"] += 1
            else:
                sl = ["/"] * n
                for k,v in ret.items():
                    dirs = map("".join, zip(sl, k[1:].split("/")))
                    yield [hostname, k, v, *dirs]
                ret.clear()
                
            cnt = n
    
def eachlocate(files, header=None, outtype="file", limit=None):
    if header:
        yield ["server", "fullpath", "count", "DIRS*"]
    for f in files:
        for d in  dumper(f, outtype, limit):
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
    #TODO
    pass

def main():
    from argparse import ArgumentParser
    usage="""
    mlocate.db text dump
       Example1: {0} *.db
       Example2: {0} *.xlsx

    """.format(os.path.basename(sys.argv[0]).replace(".py", ""))

    ps = ArgumentParser(usage)
    padd = ps.add_argument
    
    padd('-o', '--outfile', type=str, default=None,
         help='output filepath (default `stdout`)')
    padd("-l", "--limit",
         help="db dump limit lines number",
         type=int, default=None)
    padd('-t', '--type', type=str, default="file",
         help='mlocate.db dumptype. `file` or `dir`')
    padd('-r', '--regex', type=str, default=".",
         help='regex path filter string')
    padd("-H", "--header",
         help="header print",
         action='store_true', default=False)
    padd("filename",
         metavar="<filename>",
         nargs="+",  default=[],
         help="target any files(.txt, .csv, .tsv, .xls[x], .accdb, .sqlite3)")
    args = ps.parse_args()
    
    header = args.header
    outtype = args.type
    limit = args.limit
    pattern = args.regex #TODO
    
    files = [g for fn in args.filename for g in glob(fn)]
    if not files:
        raise RuntimeError("Not found files {}".format(args.filename))
    it = eachlocate(files, header, outtype, limit=limit)
    to_csv(it, args.outfile or sys.stdout)

if __name__ == "__main__":
    #sys.argv.append("mlocate.db")
    
    main()
