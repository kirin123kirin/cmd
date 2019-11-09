#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import sys
from os.path import abspath, splitext, dirname, exists, join as pathjoin
import os
from glob import glob

def which(executable):    
    for path in os.environ['PATH'].split(os.pathsep):
        path = path.strip('"')

        fpath = os.path.join(path, executable)

        if os.path.isfile(fpath) and os.access(fpath, os.X_OK):
            return fpath

    if os.name == 'nt':
        if splitext(executable)[-1]:
            return None
        else:
            wexec = [".exe", ".bat", ".cmd", ".wsh", ".vbs"]
            for ext in wexec:
                ret = which(executable + ext)
                if ret:
                    return ret

    return None


def getpypath():
    return os.getenv("PYTHONPATH", dirname(which("python")))


def repair_exe(infile):
    infile = abspath(infile)
    if not exists(infile):
        print("Not Found `{}`".format(infile), file=sys.stderr)
        return
    tmp = infile + ".tmp"
    pypath = repr(getpypath()).strip("''").encode("ascii")
    if not pypath:
        print("Not Found environment PYTHONPATH.", file=sys.stderr)
        return
    
    try:
        with open(infile, "rb") as f, open(tmp, "wb") as w:
            w.write(re.sub(br"(\n?#!)[^\r\n]+([\\/]pythonw?(?:\.exe)?\r?\n)", br"\1" + pypath + br"\2", f.read()))
    except:
        print("Rollback.... " + infile, file=sys.stderr)
        os.remove(tmp)
    else:
        print("Repairing... " + infile)
        os.remove(infile)
        os.rename(tmp, infile)

def repair_zip():
    import time

    if "JST" in time.tzname:
        from fileinput import FileInput
        import zipfile
        zf = zipfile.__file__
        
        with FileInput(zf, inplace=True, backup=".bak") as f:
            for line in f:
                print(line.replace("cp432", "cp932"), end="")

if __name__ == "__main__":
    from collections import Iterable
    def flatten(l):
        for el in l:
            if isinstance(el, Iterable) and not isinstance(el, str):
                for sub in flatten(el):
                    yield sub
            else:
                yield el

    if len(sys.argv) > 1:
        exes = [glob(pathjoin(x , "*.exe")) for x in sys.argv[1:] if exists(x)]
    else:
        exes = glob(pathjoin(getpypath(), "Scripts","*.exe"))
    for exe in flatten(exes):
        repair_exe(exe)

    repair_zip()
