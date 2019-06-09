#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from glob import glob
import sys, os, re

import plocate.mlocatedb
from future.utils import native_str
import struct

class mlocatedb(plocate.mlocatedb.mlocatedb):
    lndir = ("/etc/init.d",
        "/etc/rc0.d",
        "/etc/rc1.d",
        "/etc/rc2.d",
        "/etc/rc3.d",
        "/etc/rc4.d",
        "/etc/rc5.d",
        "/etc/rc6.d",
        "/etc/ssl/certs",
        "/etc/xdg/systemd/user",
        "/lib",
        "/lib64",
        "/sbin",
        "/usr/lib64/go/4.8.5",
        "/usr/libexec/gcc/x86_64-redhat-linux/4.8.5",
        "/bin",
        "/usr/include/c++/4.8.5",
        "/usr/lib/debug/bin",
        "/usr/lib/debug/lib",
        "/usr/lib/debug/lib64",
        "/usr/lib/debug/sbin",
        "/usr/lib/gcc/x86_64-redhat-linux/4.8.5",
        "/usr/lib/go/4.8.5",
        "/usr/lib/terminfo",
        "/usr/share/doc/git-1.8.3.1/contrib/hooks",
        "/usr/share/doc/redhat-release",
        "/usr/share/doc/vim-common-7.4.160/docs",
        "/usr/share/gcc-4.8.5",
        "/usr/share/gccxml-0.9/GCC/5.0",
        "/usr/share/gccxml-0.9/GCC/5.1",
        "/usr/share/gccxml-0.9/GCC/5.2",
        "/usr/share/gccxml-0.9/GCC/5.3",
        "/usr/share/gdb/auto-load/lib64",
        "/usr/share/groff/current",
        "/usr/tmp",
        "/var/lock",
        "/var/mail",
        "/var/run"
    )

    class _fileentry(object):
        """file entry"""
        _struct = struct.Struct(native_str('>B'))

        def __init__(self, kind, filename=None):
            super(mlocatedb._fileentry, self).__init__()
            self.kind = kind
            self.filename = filename

        def is_dir(self):
            return self.kind == 1 or self.filename in mlocatedb.lndir

        def is_endmark(self):
            return self.kind == 2

        def is_file(self):
            return self.kind == 0 and self.filename not in mlocatedb.lndir

    def next_fileentry(self, parentpath):
        """Parse file entry"""
        fe = self.reader.readnext(mlocatedb._fileentry)
        if fe.kind != 2:
            fe.filename = os.path.join(parentpath, self.reader.readcstr()[0]).replace("\\", "/") #TODO if osenv
        return fe

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
    hostname = hostname or os.path.basename(database).replace(".db", "")

    pt = re.compile(pattern).search if pattern else lambda p: True

    for o in locate(database, limit):
        x = o.filename
        tp = "dir" if o.is_dir() else "file"
        if pt(x) and (outtype == "all" or tp == outtype):
            if tp == "dir":
                dirs = map("/".__add__, x[1:].split("/"))
            else:
                dirs = map("/".__add__, x.rsplit("/", 1)[0][1:].split("/"))

            yield [hostname, tp, os.path.basename(x), x, *dirs]


def eachlocate(files, header=None, pattern=None, outtype="file", limit=None):
    if header:
        yield ["server", "type", "basename", "fullpath", "DIRS*"]
    for f in files:
        for d in dumper(f, pattern, outtype, limit):
            yield d

def to_csv(iterator, file=sys.stdout, **kw):
    import csv
    fp = file if hasattr(file, "write") else open(file, "w", encoding="utf-8")
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

        dirs = map("DIR{:02d}".format, range(sheet.dim_colmax - 3))
        sheet.write_row(0, 0, ["server", "type", "basename", "fullpath", *dirs], header_fmt)
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
    outtype = args.type
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
#    sys.argv.extend('C:/temp/mlocate.db -Ht all -l 40'.split(" "))
#    sys.argv.extend('C:/temp/mlocate.db -o C:/temp/test.csv'.split(" "))
#    sys.argv.append("C:/temp/mlocate.db")

    main()

