#!/usr/bin/env python
## -*- coding: utf-8 -*-
import os
from os.path import normpath, join as pathjoin
from os.path import isfile, isdir, basename, dirname, abspath
from glob import iglob
from datetime import datetime as dt

__author__  = "m.yama"
__license__ = 'MIT'
__version__ = "0.2.1"

__all__ = ["lsdir"]

def lsdir(wildcardpath, recursive=False, target="file", dotfileskip=True):
    """ File System Input File Path Exists Files Listing Function
    """
    for pth in iglob(wildcardpath):
        if target in (None, "d", "dir", "dirs"):
            yield pth
        for root, dirs, files in os.walk(pth):
            if target in ("d", "dir", "dirs"):
                lp = dirs
            elif target in ("f", "file", "files"):
                lp = files
            else:
                lp = dirs + files
            
            if dotfileskip is True:
                lp = (x for x in lp if not x.startswith("."))
                
            for file in lp:
                yield os.path.join(root, file)


def bytes_human(num):
    for x in ['B','KB','MB','GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')
    
if __name__ == "__main__":
    import argparse
    import sys
    #sys.argv.extend("-rt d /storage/emulated/0/Pictures/".split(" "))
    parser = argparse.ArgumentParser()
    parser.add_argument('-t',
                        '--type',
                        help='filter file type , file or dir  (default both)',
                        default=None)
    parser.add_argument('-o',
                        '--outfile',
                        help='output filepath (default STDOUT)',
                        default=sys.stdout)
    parser.add_argument('-s',
                        '--sep',
                        help='output file field separator',
                        default=',')
    parser.add_argument('-r',
                        '--recursive', action='store_true', default=False,
                        help='directories recursive')
    parser.add_argument('-d',
                        '--dotfile', action='store_true', default=False,
                        help='printing dotfile')
    parser.add_argument(
                        '--human', action='store_true', default=False,
                        help='size number for human read easily')
    parser.add_argument('filename',
                        metavar='<filename>',
                        nargs=1,
                        help='Target File')
    args = parser.parse_args()
    
    t = lsdir(wildcardpath=args.filename[0], recursive=args.recursive, target=args.type, dotfileskip=args.dotfile is False)
    dp = lambda x: dt.fromtimestamp(x).strftime("%Y/%m/%d %H:%M")
    
    logger = lambda x: print(*x, sep=args.sep, file=args.outfile, flush=True)
    
    mx = 0
    res = []
    for f in t:
        s=os.stat(f)
        fn   = abspath(f)
        ext = os.path.splitext(fn)[-1].lower().replace(".", "")
        dn, bn  = os.path.split(fn)
        sp = dirname(f).replace("\\", "/").split("/")
        try:
            sp.remove("")
        except:
            pass
        if args.human:
            sz = bytes_human(s.st_size)
        else:
            sz = s.st_size
        if mx < len(sp):
            mx = len(sp)
        ret = [fn, dn, bn, ext, oct(s.st_mode)[-3:], sz, dp(s.st_ctime), dp(s.st_mtime), "|" ,*sp]
        res.append(ret)

    hr = args.sep.join(["DIR{}".format(i) for i in range(1,mx+1)])
    logger(("FULLpath,DIRpath,FILEname,EXTention,PERMission,FILESIZE,CREATE_date,MODIFIED_date,|,"+hr).split(","))
    for r in res:
        logger(r)
