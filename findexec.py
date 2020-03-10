#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from glob import glob
from itertools import chain
from fnmatch import fnmatch
import shutil
from os.path import isdir, join as pathjoin

__doc__     = "Summary: find and executable library"
__author__  = 'm.yama'
__license__ = 'MIT'
__date__    = 'Mon Mar  9 11:53:23 2020'
__version__ = '0.0.1'
__all__     = [
    "findexec",
]


def findexec(exec, name="*", ignore_name=None, type="both"):
    """
    Each find file execute function. :decorator function

    Like is Unix `find`

    Parameters:
        :exec: execute finction callable function
        :name: file or directory name pattern(wildcard string)
        :ignore_name: ignore name pattern(wildcard string)
        :type: file or dir or both

    Example:
        >>> @findexec(name="old*", type="file")
            def rm_rf_old_files(target_directory):
                os.remove(target_directory)
                print("delete", target_directory)
        >>> rm_rf_old_files("/tmp")
    """

    def it(pathes):
        if isinstance(pathes, (str, bytes)):
            for p in os.listdir(pathes):
                yield [p, pathjoin(pathes, p)]
        elif hasattr(pathes, "__next__") or hasattr(pathes, "__iter__"):
            for lp in pathes:
                for p in os.listdir(lp):
                    yield [p, pathjoin(lp, p)]
        else:
            raise ValueError("Unkown `{}`".format(pathes))

    if not ignore_name:
        if isinstance(name, (str, bytes)):
            def ismatch(p):
                return fnmatch(p, name)
        elif hasattr(name, "__next__") or hasattr(name, "__iter__"):
            def ismatch(p):
                for n in name:
                    if fnmatch(p, n):
                        return True
                return False
    else: #TODO list
        def ismatch(p):
            return fnmatch(p, name) and not fnmatch(p, ignore_name)

    tp = type.lower()[0] if type else "b"

    if tp == "b":
        def findexec_both(_directory, verbose=False):
            for p, np in it(_directory):
                if ismatch(p):
                    if verbose:
                        print("Run:{}({})".format(exec.__name__, np))
                    exec(np)
                elif isdir(np):
                    findexec_both(np)

        return findexec_both

    elif tp == "f":
        def findexec_file(_directory, verbose=False):
            for p, np in it(_directory):
                if isdir(np):
                    findexec_file(np)
                elif ismatch(p):
                    if verbose:
                        print("Run:{}({})".format(exec.__name__, np))
                    exec(np)

        return findexec_file

    elif tp == "d":
        def findexec_dir(_directory, verbose=False):
            for p, np in it(_directory):
                if isdir(np):
                    if ismatch(p):
                        if verbose:
                            print("Run:{}({})".format(exec.__name__, np))
                        exec(np)
                    else:
                        findexec_dir(np)

        return findexec_dir

    else:
        raise ValueError("Unkown type value: `{}`.\nPlease type is `file` `dir` `both`".format(type))


def create_parser(lowlevel=False):
    from argparse import ArgumentParser

    parser = ArgumentParser(description="find execute comman")
    padd = parser.add_argument

    padd('-v', '--verbose',
         action='store_true', default=False,
         help="run command stdout",
    )

    if lowlevel:
        padd('-e', '--exec',
            nargs="*",
            help='execute command',
            default=None)

        padd('-t', '--type',
            help='filter file type , file=>f or dir=>d or both=>b (default both)',
            default="b")

        padd('-n', '--name', #TODO list
            help='wildcard string for filter(like is `find -name`)',
            default="*")
        
        padd('-i', '--ignore_name',
            help='wildcard string for ignore filter(like is `find -not -name`)',
            default=None)
            
    padd('directories',
         metavar='<directories>',
         #nargs="*", default=[["."]],
         nargs="+",
         type=glob,
         help='Execute target directories',
    )

    args = parser.parse_args()
    args.directories = list(filter(isdir, chain(*args.directories)))
    if not args.directories:
        parser.error("Directories Not Found. Please check directories")
    if lowlevel and args.exec:
        args.exec = " ".join(args.exec).replace("\\", "").strip("'\"")
        
    return args


def main_rmdotdir():
    args = create_parser()
    findexec(shutil.rmtree, name=".*", type="dir")(args.directories, args.verbose)


def main_rmdotfile():
    args = create_parser()
    findexec(os.remove, name=".*", type="file")(args.directories, args.verbose)


def main_rmgit():
    args = create_parser()
    findexec(shutil.rmtree, name=".git", type="dir")(args.directories, args.verbose)


def main_rmsvn():
    args = create_parser()
    findexec(shutil.rmtree, name=".svn", type="dir")(args.directories, args.verbose)


def main():
    from subprocess import getstatusoutput

    args = create_parser(True)
    verb = args.verbose

    def oscommand(_target):
        cmd = "{} {}".format(args.exec, _target)
        if verb:
            print("Run:", cmd)
        code, dat = getstatusoutput(cmd)
        if code == 0:
            print(dat)
        else:
            print(dat, file=sys.stderr)

    findexec(oscommand if args.exec else print, name=args.name, ignore_name=args.ignore_name, type=args.type)(args.directories)


def test():
    from datetime import datetime as dt
    from tempfile import TemporaryDirectory

    def addarg(x):
        sys.argv = sys.argv[:1] + x.split(" ")

    def mkdir(x):
        if not os.path.exists(x):
            os.mkdir(x)

    with TemporaryDirectory() as tmpdir:
        def maketestdata():
            from pathlib import Path
            touch = lambda x: Path(x).touch()
            mkdir(tmpdir)
            mkdir(pathjoin(tmpdir, ".foo"))
            mkdir(pathjoin(tmpdir, ".svn"))
            mkdir(pathjoin(tmpdir, ".git"))
            touch(pathjoin(tmpdir, ".bar"))
            touch(pathjoin(tmpdir, "hoge.txt"))

        def test_rmdotfile():
            addarg("-v " + tmpdir)
            maketestdata()
            main_rmdotfile()
            assert sorted(os.listdir(tmpdir)) == ['.foo', '.git', '.svn', 'hoge.txt']

        def test_rmdotdir():
            addarg("-v " + tmpdir)
            maketestdata()
            main_rmdotdir()
            assert sorted(os.listdir(tmpdir)) == ['.bar', 'hoge.txt']

        def test_rmgit():
            addarg("-v " + tmpdir)
            maketestdata()
            main_rmgit()
            assert sorted(os.listdir(tmpdir)) == ['.bar', '.foo', '.svn', 'hoge.txt']

        def test_rmsvn():
            addarg("-v " + tmpdir)
            maketestdata()
            main_rmsvn()
            assert sorted(os.listdir(tmpdir)) == ['.bar', '.foo', '.git', 'hoge.txt']

        def test_main_dir():
            addarg("-v -e dir -t d -n .* " + tmpdir)
            maketestdata()
            main()
            
        def test_main_file():
            addarg("-v -e dir -t f -n .* " + tmpdir)
            maketestdata()
            main()
        
        def test_main_file_ignore():
            addarg("-v -i h* -e 'ls \-l' -t f " + tmpdir)
            maketestdata()
            main()

        for x, func in list(locals().items()):
            if x.startswith("test_") and callable(func):
                t1 = dt.now()
                func()
                t2 = dt.now()
                print("{} : time {}".format(x, t2-t1))

if __name__ == "__main__":
    #test()
    main()
