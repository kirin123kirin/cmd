#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime as dt
import os
import csv
import sys
import re
import codecs
from glob import glob
import zipfile, gzip, bz2, lzma, tarfile
import fnmatch

from util.core import (lsdir, 
dirstree,
filestree,
getsize,
timestamp2date,
path_norm,
in_glob,
getinfo,
istar,
iszip,
isarc,
iszlib,
iscompress,
compmap,
filemap,
HEADER
)


def zlibarg(f):
    sfx = Path(f).suffix.lower()
    cls = None
    try:
        cls, comp = filemap[sfx], compmap[sfx]
    except KeyError:
        raise ValueError("Unknown extention " + sfx)
    except:
        comp = sfx.strip(".")
    with cls(f) as z:
        fn = Path(os.path.join(f, os.path.basename(f).strip(sfx)))
        yield [str(fn), fn.parent, fn.name, sfx, "-", "-", "-", "-", z.mtime and timestamp2date(z.mtime) or "-", getsize(z)]


def ziparg(f, target=[]):
    with zipfile.ZipFile(f) as z:
        target = in_glob(z.namelist(), target)
        for info in z.infolist():
            if info.is_dir():
                continue
            
            if info.filename in target:
                fn = Path(os.path.join(f, info.filename))
                t = dt(*info.date_time).strftime("%Y/%m/%d %H:%M")
                yield [str(fn), fn.parent, fn.name, fn.suffix.lower(),"-", "-", "-", "-" , t, info.file_size]

def tararg(f, target=[]):
    with tarfile.open(f) as z:
        target = in_glob(z.getnames(), target)
        for info in z.getmembers():
            if info.isdir():
                continue
            if info.name in target:
                fn = Path(os.path.join(f, info.name))
                yield [str(fn), fn.parent, fn.name, fn.suffix.lower(), info.uname, info.gname, oct(info.mode)[-3:], "-", timestamp2date(info.mtime), info.size]

def lscompress(f:str, writer, recursive:bool=True, dotfile=False, header=True):
    if header:
        writer.writerow(HEADER)
    pre, inner = path_norm(f)
    for gf in glob(pre):
        if iszip(gf):
            gen = ziparg
        elif istar(gf):
            gen = tararg
        elif iszlib(gf):
            gen = zlibarg
        else:
            raise ValueError("Unknown Format")

        for a in gen(gf):
            sp = a[0].split(os.path.sep)
            if dotfile is False or all(not x.startswith(".") for x in sp):
                writer.writerow(a + ["|"] + sp[:-1])


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
        for i, af in enumerate(args.filename):
            j = 0
            for gf in glob(af):
                #gf = os.path.normpath(gf)
                if iscompress(gf) and args.extract:
                    lscompress(gf,
                         writer = writer,
                         recursive = args.recursive,
                         dotfile = args.dotfile,
                         header = i + j == 0)
                else:
                    func(gf,
                         writer = writer,
                         recursive = args.recursive,
                         dotfile = args.dotfile,
                         header = i + j == 0)
                j += 1
            if j == 0:
                sys.stderr.write("Not Found Path " + str(af))
                             
    if args.type.lower().startswith("f"):
        render(filestree)
    else:
        render(dirstree)



