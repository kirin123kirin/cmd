#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

list directory infomation tools

MIT License

"""

__version__ = "0.0.3"
__author__ = "m.yama"


__all__ = ["fwalk", "dwalk", "filestree", "dirstree", "filemode"]


import sys
import os
from os.path import basename, splitext, abspath, isdir
from os import scandir, fspath, readlink

from datetime import datetime
from functools import lru_cache
from glob import glob

if os.name == "posix":
    from pwd import getpwuid
    from grp import getgrgid

    @lru_cache()
    def getuser(uid):
        try:
            return getpwuid(uid).pw_name
        except KeyError:
            return uid

    @lru_cache()
    def getgroup(gid):
        try:
            return getgrgid(gid).gr_name
        except KeyError:
            return gid
else:
    def getuser(uid):
        return ""
    def getgroup(gid):
        return ""

@lru_cache()
def ts2date(x, dfm = "%Y/%m/%d %H:%M"):
    return datetime.fromtimestamp(x).strftime(dfm)


def fwalk(top, exclude=None, followlinks=False):
    scandir_it = scandir(fspath(top))

    with scandir_it:
        while True:
            try:
                entry = next(scandir_it)
                if exclude and entry.name in exclude:
                    continue
            except StopIteration:
                break

            try:
                is_dir = entry.is_dir()
            except OSError:
                is_dir = False

            if is_dir:
                if followlinks and entry.is_symlink():
                    yield from fwalk(readlink(entry.path), exclude, followlinks)
                else:
                    yield from fwalk(entry.path, exclude, followlinks)
            else:
                yield entry


def dwalk(top, exclude=None, followlinks=False):
    scandir_it = scandir(fspath(top))

    with scandir_it:
        while True:
            try:
                entry = next(scandir_it)
                if exclude and entry.name in exclude:
                    continue
            except StopIteration:
                break

            try:
                is_dir = entry.is_dir()
            except OSError:
                is_dir = False

            if is_dir:
                if followlinks and entry.is_symlink():
                    yield from dwalk(readlink(entry.path), exclude, followlinks)
                else:
                    yield from dwalk(entry.path, exclude, followlinks)

_filemode_table = (
    ((0o120000,         "l"),
     (0o140000,        "s"),  # Must appear before IFREG and IFDIR as IFSOCK == IFREG | IFDIR
     (0o100000,         "-"),
     (0o060000,         "b"),
     (0o040000,         "d"),
     (0o020000,         "c"),
     (0o010000,         "p")),

    ((0o0400,         "r"),),
    ((0o0200,         "w"),),
    ((0o0100|0o4000, "s"),
     (0o4000,         "S"),
     (0o0200,         "x")),

    ((0o0040,         "r"),),
    ((0o0200,         "w"),),
    ((0o0010|0o2000, "s"),
     (0o2000,         "S"),
     (0o0010,         "x")),

    ((0o0004,         "r"),),
    ((0o0002,         "w"),),
    ((0o0001|0o1000, "t"),
     (0o1000,         "T"),
     (0o0001,         "x"))
)

@lru_cache()
def filemode(mode):
    perm = []
    add = perm.append
    for table in _filemode_table:
        for bit, char in table:
            if mode & bit == bit:
                add(char)
                break
        else:
            add("-")
    return "".join(perm)


def fileattr(f, stat=None):
    stat = stat or os.stat(f)
    return (
            filemode(stat.st_mode),
            getuser(stat.st_uid),
            getgroup(stat.st_gid),
            ts2date(stat.st_mtime),
            stat.st_size,
            splitext(f)[1],
            basename(f),
            f,
            *f.replace("\\", "/").strip("/").split("/"),
            )

def _tree(func, fn, exclude=None, followlinks=False, header=True):

    i = 0

    for g in glob(abspath(fn)):#TODO readlink?
        if i == 0 and header:
            yield ["mode", "uname", "gname", "mtime", "size", "ext", "name", "fullpath", "link", "dirnest"]

        if isdir(g):
            for f in func(g, exclude, followlinks):
                yield fileattr(f.path, f.stat())
                i += 1

        else:
            yield fileattr(g)
            i += 1

    if i == 0:
        raise FileNotFoundError(fn)

def filestree(fn, exclude=None, followlinks=False, header=True):
    return _tree(fwalk, fn=fn, exclude=exclude, followlinks=followlinks, header=header)

def dirstree(fn, exclude=None, followlinks=False, header=True):
    return _tree(dwalk, fn=fn, exclude=exclude, followlinks=followlinks, header=header)

def main():
    from argparse import ArgumentParser
    import io
    import codecs

    parser = ArgumentParser(description="main templace")
    padd = parser.add_argument

    padd('-o', '--outfile',
         type=lambda f: lambda e: codecs.open(f, mode="w", encoding=e, errors="replace"),
         help='outputfile path',
         default=lambda e: io.TextIOWrapper(sys.stdout.buffer, encoding=e, errors="replace"),
    )
    
    padd('-t',
        '--type',
        help='filter file type , file=>f or dir=>d (default file)',
        default="f")

    padd('-s', '--sep',
         help='output separator',
         default="\t",
    )

    padd('-E', '--exclude', nargs='+',
         help='exclude files',
         default=[".svn", ".git", "old", "bak"],
    )

    padd('-N', '--noheader',
         action='store_false', default=True,
         help='output no header',
    )

    padd('-e', '--encoding',
        help='output encoding',
        default=os.name == "nt" and "cp932" or "utf-8",
    )

    padd('filename',
         metavar='<filename>',
         nargs="+",
         help='Target Files',
    )

    args = parser.parse_args()

    func = dict(f=filestree, d=dirstree)[args.type.lower()[0]]

    with args.outfile(args.encoding) as outfile:
        for i, filename in enumerate(args.filename):
            for row in func(filename, exclude=args.exclude, header=args.noheader and i == 0):
                print(args.sep.join(row), file=outfile)


def test():

    def test_filestree():
        assert len(list(filestree("."))) > 0


    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = datetime.now()
            func()
            t2 = datetime.now()
            print("{} : time {}".format(x, t2-t1))


if __name__ == '__main__':
#    test()
    main()



