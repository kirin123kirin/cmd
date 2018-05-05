# -*- coding: utf-8 -*-
import os, sys
#import pyodbc
#import pandas as pd
#from datetime import datetime
from tempfile import gettempdir
from msutils import Excel, MSDB
from differ import Differ, flatten, logger

__version__ = "0.0.2"

BUFSIZE = 5 * 1024 * 1024
SEP = ","
LINESEP = "\n"

SUMARY = """
#### {title} Diff Summary ###
# {log}
#   - equal   : {equal:7} lines
#   - replace : {replace:7} lines
#   - delete  : {delete:7} lines
#   - insert  : {insert:7} lines
"""

todoclean = []


sepjoin = lambda *x: SEP.join(str(y) for y in flatten(x) if not isinstance(y, str)) + LINESEP
values_at = lambda d, k: dict((x, d[x]) for x in set(d) & set(k))
values_not = lambda d, k: dict((x, d[x]) for x in set(d) - set(k))

def txtdiff(filename1, filename2, skipequal=True, logfilepath=None, sort=False,header=True):
    with open(filename1) as f1, open(filename2) as f2, logger(logfilepath) as w:
        w.write(LINESEP.join(["# beforefile: " + filename1, "# afterfile : " + filename2]) + LINESEP)
        dd = Differ(f1, f2, skipequal=skipequal, key=sort is True and str or None, header=header)
        dd.pprint(w, title=sort and "Text by line sort" or "Text", linesep="\n")

def struct_diff(f1, f2, w, title="", **kw):
    changed = {}

    rkw = values_not(kw, ["sort", "skipequal", "key"])
    a = "f1.{}s()".format(title.lower())
    b = "f2.{}s()".format(title.lower())

    for tag, tar,_,_ in Differ(eval(a), eval(b),skipequal=False).compare():
        sh1, sh2 = (tar.split(" ---> ") + [None])[0:2]
        g1 = f1.readlines(sh1, **rkw)
        g2 = f2.readlines(sh2 or sh1, **rkw)        
        dd = Differ(g1, g2, **values_at(kw, ["skipequal","header","key","in_memory","isjunk"]))
        
        # make Summary
        changed[tar] = dict(title='{}="{}"'.format(title, tar),equal=0, delete=0,insert=0, replace=0)
        mklog = lambda f: "{} lines x {} cols".format(f.nrow(tar), f.ncol(tar))

        # make Detail
        if tag in ["replace", "equal"]:
            w.write(dd.detail(title=title + " " + tar))
            changed[tar].update(dd.result)
            if dd.is_same():
                changed[tar]["log"] = "[!{}!] {} ({})".format(tag.upper(), tar, mklog(f1))
            else:
                changed[tar]["log"] = "[!{}!] {} ({} ---> {})".format("REPLACE", tar, mklog(f1), mklog(f2))

        else:        
            if tag == "delete":  ff, gg = f1, g1
            if tag == "insert":  ff, gg = f2, g2

            for i, line in enumerate(gg):
                w.write(sepjoin(tag, tar,i+1,"-",line))

            changed[tar].update(dict(tag=i))
            changed[tar]["log"] = "[!{}!] {} ({})".format(tag.upper(), tar, mklog(ff))

        
    for tar, val in changed.items():
        w.write(SUMARY.format(**val))


def exceldiff(filename1, filename2, skipequal=True, logfilepath=None, sort=False, header=True, skiprows=None, **kw):
    with Excel(filename1) as f1, Excel(filename2) as f2, logger(logfilepath) as w:
        struct_diff(f1, f2, w, title="Sheet", skipequal=skipequal, header=header, key=sort is True and str or None)

def mdbdiff(filename1, filename2, skipequal=True, logfilepath=None, sort=False, header=True, skiprows=None, **kw):
    with MSDB(filename1) as f1, MSDB(filename2) as f2, logger(logfilepath) as w:
        struct_diff(f1, f2, w, title="Table", skipequal=skipequal, header=header, key=sort is True and str or None)

def sqldiff(sql1, sql2, skipequal=True, logfilepath=None, n=5, sort=False):
    pass

def clipdiff(skipequal=True, logfilepath=None, n=5, sort=False):
    pass

def xmldiff(skipequal=True, logfilepath=None, n=5, sort=False):
    pass

def typehelper(filename1, filename2, *args, **kw):
    ext1 = os.path.splitext(filename1)[-1].lower()
    ext2 = os.path.splitext(filename2)[-1].lower()
    if ext1.startswith(".xls") and ext2.startswith(".xls"):
        return exceldiff(filename1, filename2, *args, **kw)
    if ext1 in [".accdb", ".mdb"] and ext2 in [".accdb", ".mdb"]:
        return mdbdiff(filename1, filename2, *args, **kw)
    if ext1 in [".txt", ".tsv", ".csv"] and ext2 in [".txt", ".tsv", ".csv"]:
        return txtdiff(filename1, filename2, *args, **kw)
    if ext1 in [".txt", ".tsv", ".csv"] and ext2.startswith(".xls"):
        tmp = Excel(filename2).dump(0, os.path.join(gettempdir(), filename2+ext2))
        todoclean.append(tmp)
        return txtdiff(filename1, tmp, *args, **kw)
    if ext1.startswith(".xls") and ext2 in [".txt", ".tsv", ".csv"]:
        tmp = Excel(filename1).dump(0, os.path.join(gettempdir(), filename1+ext1))
        todoclean.append(tmp)
        return txtdiff(tmp, filename2, *args, **kw)
    if ext1 in [".txt", ".tsv", ".csv"] and ext2 in [".accdb", ".mdb"]:
        table = os.path.basename(os.path.splitext(filename2)[0])
        tmp = MSDB(filename2).dump(table, os.path.join(gettempdir(), filename2+ext2))
        todoclean.append(tmp)
        return txtdiff(filename1, tmp, *args, **kw)
    if ext1 in [".accdb", ".mdb"] and ext2 in [".txt", ".tsv", ".csv"]:
        table = os.path.basename(os.path.splitext(filename1)[0])
        tmp = MSDB(filename1).dump(table, os.path.join(gettempdir(), filename1+ext1))
        todoclean.append(tmp)
        return txtdiff(tmp, filename2, *args, **kw)
    if ext1+ext2 == ".sql.sql":
        return sqldiff(filename1, filename2, *args, **kw)

    raise RuntimeError("Unknown filetype")

def clean():
    for tmp in todoclean:
        os.remove(tmp)

usage = "%(prog)s"

def main():
    import argparse
    import traceback
    parser = argparse.ArgumentParser(prog="mdiff", description="2 file diff compare program\n" + usage)
    parser.add_argument('-V','--version', action='version', version='%(prog)s ' + __version__)
    parser.add_argument('file1', nargs=1, help='diff before file')
    parser.add_argument('file2', nargs=1, help='diff after file')
    
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Progress verbose output.')
    parser.add_argument('-f', '--full', action='store_true', default=False,
                        help='Full Line print(default False)')
    parser.add_argument('-o', '--outfile', type=str, default=None,
                        help='output filepath (default `stdout`)')
    parser.add_argument('-s', '--sort', action='store_true', default=False,
                        help='before diff file Sorted.')
    parser.add_argument('-H', '--header', action='store_true', default=False,
                        help='file no header (default `False`) (sorted option)')

    args = parser.parse_args()
    
    try:            
        typehelper(filename1=args.file1[0],
                   filename2=args.file2[0],
                   skipequal=args.full is False,
                   logfilepath=args.outfile,
                   sort=args.sort,
                   header=args.header
                   )
    except Exception as e:
        traceback.print_exc()
    finally:
        clean()

def test():
    a = ["1222222","123","123"]
    b = ["1222223","122","","123"]

    a = [["1222222","123","123"],list("222")]
    b = [["1222223","122","","123"]]
    a=["twoooooooo", "1","1222222","333","444"]
    b=["zero", "1", "1322222","333","","555"]


if __name__ == "__main__":
#    test()
#    sys.argv.extend("-sf C:/temp/hoge.csv C:/temp/hoge_after.csv".split(" "))
#    sys.argv.extend("-sf C:/temp/diff1.xlsx C:/temp/diff2.xlsx".split(" "))
#    sys.argv.extend("-s C:/temp/t/old.xlsx C:/temp/t/new.xlsx".split(" "))
#    sys.argv.extend("C:/temp/diff1.xlsx C:/temp/diff2.xlsx".split(" "))
#    sys.argv.extend("C:/temp/sample.accdb C:/temp/diff2.accdb".split(" "))
#    sys.argv.append("-h")
    main()
