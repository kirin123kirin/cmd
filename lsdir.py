#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import csv
import sys
import codecs

from util.core import lsdir

def filestree(path, recursive=True):
    for x in lsdir(path, recursive):
        if x.is_file():
            yield x

def dirstree(path, recursive=True):
    for x in lsdir(path, recursive):
        if x.is_dir():
            yield x

if __name__ == "__main__":
    import argparse
#    sys.argv.extend(r"-rt f --extract /storage/emulated/0/Download/diff1.*".split(" "))
    parser = argparse.ArgumentParser()
    parser.add_argument('-t',
                        '--type',
                        help='filter file type , file=>f or dir=>d (default file)',
                        default="f")
    parser.add_argument('-e',
                        '--encoding',
                        help='output encoding',
                        default=os.name == "nt" and "cp932" or "utf-8")
    parser.add_argument('-o',
                        '--outfile',
                        help='output filepath (default STDOUT)',
                        default=None)
    parser.add_argument(
                        '--extract', action='store_true', default=False,
                        help='extract print of gzip zip tar bz2 files')
    parser.add_argument('-r',
                        '--recursive', action='store_true', default=False,
                        help='directories recursive')
    parser.add_argument('-d',
                        '--dotfile', action='store_true', default=False,
                        help='printing dotfile')
    parser.add_argument('filename',
                        metavar='<filename>',
                        nargs="+",
                        help='Target File')
    args = parser.parse_args()
    
    sep = os.path.sep
    if args.outfile:
        output = codecs.open(args.outfile, "w", encoding=args.encoding)
    else:
        output = sys.stdout
    writer = csv.writer(output, quoting=csv.QUOTE_ALL)
    
    def render(func):
        i = 0
        for p in func(args.filename[0], args.recursive):
            if args.dotfile or all(not x.startswith(".") for x in p.parts):
                r = p.getinfo()
                if i == 0:
                    writer.writerow(r._fields)
                writer.writerow(list(r))
                i += 1
        try:
            i
        except UnboundLocalError:
            sys.stderr.write("Not Found Path " + str(p))
            sys.exit(1)
                             
    if args.type.lower().startswith("f"):
        render(filestree)
    else:
        render(dirstree)



