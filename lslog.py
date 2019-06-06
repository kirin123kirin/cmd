#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pandas._libs.tslibs.parsing import parse_time_string
import os, re, sys
from datetime import datetime
from io import StringIO
import codecs
from pathlib import Path

def parse_date(s, base_date = "", future_date=True,
        mod_d = re.compile("\s*[年月日]\s*"),
        mod_t = re.compile("\s*[時分秒]\s*"),
        callback=None):
    
    s = mod_d.sub("/", s)
    s = mod_t.sub(":", s)
    
    dt =parse_time_string(s)[0]
    
    if dt.year == 1:
        
        if base_date:
            b = mod_d.sub("/", base_date)
            b = mod_t.sub(":", b)
            base_date = parse_time_string(b)[0]
        else:
            base_date = datetime.now()
        
        check = dt.replace(year=base_date.year)
        
        if future_date is False and check > base_date:
            dt = dt.replace(year=base_date.year - 1)
        else:
            dt = check
    
    return callback(dt) if callback else dt

def split_date_time(dt):
    return dt.strftime("%Y/%m/%d"), dt.strftime("%H:%M:%S")

def parse_ls(line, parent="", base_date="", mod_sp = re.compile(" +")):
    if line.startswith("l"):
        p, _, o, g, size, *d, name, _, ln = mod_sp.split(line)
    else:
        ln = ""
        p, _, o, g, size, *d, name = mod_sp.split(line)
    
    dt = parse_date(" ".join(d), base_date, future_date=False, callback=split_date_time)
    
    ret = [p, o, g, size, *dt, name.split(".")[-1] if "." in name else "", name]
    if not parent:
        return [*ret, ln]
    fullpath = parent.rstrip("/") + "/" + name
    dirname = fullpath if p.startswith("d") else parent.rstrip("/")
    return [*ret, fullpath, ln, *map("/".__add__, dirname.split("/"))]
    
def opener(f, *args, **kw):
    if hasattr(f, "read"):
        return f
    if isinstance(f, (str, Path)) and os.path.exists(f):
        return codecs.open(f, *args, **kw)
    return StringIO(f)

def lslog(lines, parent = "", base_date=""):
    exist_p = re.compile("^[^\s].*:$").match
    exist_c = re.compile(r"^[drwxtslc\-]{10} .*").match
    
    current = parent
    for line in lines:
        if not line:
            continue
        
        line = line.rstrip()
        
        if exist_c(line):
            yield parse_ls(line, current, base_date)
        elif exist_p(line):
            if parent:
                current = parent + line.strip(" :").lstrip(".")
            else:
                current = line.strip(" :")

HEADER = ["permission", "owner", "group", "size", "date", "time", "extention", "basename", "fullpath", "link_path"]

def to_csv(iterator, file=sys.stdout, header=HEADER, **kw):
    import csv
    fp = file if hasattr(file, "write") else open(file, "w")
    if fp == sys.stdout:
        kw["lineterminator"] = "\n"
    writer = csv.writer(fp, **kw)
    if header:
        writer.writerow(header + ["DIRS*"])
    for x in iterator:
        writer.writerow(x)
        fp.flush()
    if not hasattr(file, "write"):
        fp.close()

def to_excel(iterator, file, header=HEADER):
    from xlsxwriter import Workbook
    with Workbook(file) as book:
        sheet = book.add_worksheet()
        for i, row in enumerate(iterator, 1):
            sheet.write_row(i, 0, row)
        if header:
            sheet.write_row(0, 0, [*header, *range(sheet.dim_colmax - 9)])

def main():
    from argparse import ArgumentParser
    from glob import glob
    
    usage="""
    parse from `ls -lR` log string
       Example1: {0} *.log -o fileslist.csv
       Example2: {0} *.log -o fileslist.xlsx

    """.format(os.path.basename(sys.argv[0]).replace(".py", ""))

    ps = ArgumentParser(usage)
    padd = ps.add_argument
    
    padd('-o', '--outfile', type=str, default=None,
         help='output filepath (default `stdout`)')
    padd('-b', '--basedate', type=str, default=None,
         help='run ls -lR Date')
    padd('-c', '--currentdirectory', type=str, default=None,
         help='run ls -lR Current Directory Path String')
    padd("filename",
         metavar="<filename>",
         nargs="+",  default=[],
         help="target mlocate.db files")
    args = ps.parse_args()
    
    outfile = args.outfile or sys.stdout
    BASE_DATE = args.basedate
    PARENT = args.currentdirectory
    
    files = [g for fn in args.filename for g in glob(fn)]
    if not files:
        raise RuntimeError("Not found files {}".format(args.filename))
    
    it = (x for file in files for x in lslog(opener(file), PARENT, BASE_DATE))
    
    if str(outfile).lower().rsplit(".", 1)[1].startswith("xls"):
        to_excel(it, outfile)
    else:
        to_csv(it, outfile)
    

if __name__ == "__main__":
    #sys.argv.extend('lsdir.log -c /hoge/foo -o test.xlsx'.split(" "))
    
    main()
