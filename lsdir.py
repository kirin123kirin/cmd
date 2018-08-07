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

dfm = "%Y/%m/%d %H:%M"
def timestamp2date(x):
    return dt.fromtimestamp(x).strftime(dfm)

def lsdir(f:str, recursive:bool=True):
    p = Path(f)
    if recursive and p.is_dir():
        for root, dirs, files in os.walk(p):
            yield Path(root)
            for file in files:
                yield Path(os.path.join(root, file))
    elif p.is_file() or p.is_dir():
        yield p
    else:
        for x in p.glob("*"):
            yield x

HEADER = "fullpath,parent,basename,extention,owner,group,permision,cdate,mdate,filesize".split(",") + ["DIR"+str(i) for i in range(1,11)]
def getinfo(x):
    st = x.stat()
    pdir = x.parent
    return [
            str(x),                       # fullpath
            str(pdir),                    # parent dir
            x.name,                       # basename
            x.suffix.lower(),             # extention
            isposix and x.owner() or "-", # owner
            isposix and x.group() or "-", # group
            oct(st.st_mode)[-3:],         # permision
            timestamp2date(st.st_ctime),  # create date
            timestamp2date(st.st_mtime),  # modified date
            st.st_size,                   # file size
            "|",
            ] + list(pdir.parts)          # directories hieralchy

isposix = os.name == "posix"
def filestree(f:str, writer, recursive:bool=True, dotfile=False, header=True):
    if header:
        writer.writerow(HEADER)
    for x in lsdir(f, recursive):
        if dotfile is False and (x.name.startswith(".") or os.path.sep + "." in str(x)):
            continue
        if x.is_file():
            writer.writerow(getinfo(x))

def dirstree(f:str, writer, recursive:bool=True, dotfile=False, header=True):
    if header:
        writer.writerow(HEADER)
    for x in lsdir(f, recursive):
        if x.is_dir():
            if dotfile is False and (x.name.startswith(".") or os.path.sep + "." in str(x)):
                continue
            writer.writerow(getinfo(x))

def getsize(fp):
    p = fp.tell()
    fp.seek(0, os.SEEK_END)
    size = fp.tell()
    fp.seek(p)
    return size

re_tar = re.compile(r"(\.tar|\.tgz|\.tz2)", re.I)
re_zip = re.compile(r"(\.zip)", re.I)
re_arc = re.compile(r"(\.tar|\.tgz|\.tz2|\.zip)", re.I)
re_zlib = re.compile(r"(\.gz|\.gzip|\.bz2|\.xz|\.lzma)", re.I)
re_zsplit=re.compile(r"(.+(?:{}))[/\\]?(.*)".format(
                                    r"|".join(r.pattern.strip("()") for r in [re_zlib, re_arc])
                                    ), re.I)

istar = lambda x: re_tar.search(x)
iszip = lambda x: re_zip.search(x)
isarc = lambda x: re_arc.search(x)
iszlib = lambda x: re_zlib.search(x)
iscompress = lambda x: re_zsplit.search(x)

compmap = {".gz": "gzip", ".bz2": "bz2", ".xz": "xz", ".lzma": "xz"}
filemap = {".gz": gzip.GzipFile, ".bz2": bz2.BZ2File, ".xz": lzma.LZMAFile, ".lzma": lzma.LZMAFile}
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

def in_glob(srclst, wc):
    if not wc:
        return srclst
    ret = []
    for w in wc:
        ret.extend(fnmatch.filter(srclst, w))
    return ret

def ziparg(f, target=[]):
    with zipfile.ZipFile(f) as z:
        target = in_glob(z.namelist(), target)
        for info in z.infolist():
            if info.is_dir():
                continue
            
            if info.filename in target:
                fn = Path(os.path.join(f, info.filename))
                t = dt(*info.date_time).strftime(dfm)
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

def path_norm(f):
    rm = re_zsplit.search(f)
    if rm:
        a, b = rm.groups()
        return a, b and [b] or []
    else:
        return f, []

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



