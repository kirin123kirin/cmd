#!/usr/bin/env python
## -*- coding: utf-8 -*-
import os
from os.path import normpath, join as pathjoin
from os.path import isfile, isdir, basename, dirname
from glob import glob

__author__  = "m.yama"
__license__ = 'MIT'
__version__ = "0.1.2"

__all__ = ["lsdir"]

def lsdir(wildcardpath, recursive=False, target="file", dotfileskip=True):
    """ File System Input File Path Exists Files Listing Function
    """
    nodes = glob(wildcardpath)
    if recursive is False:
        for node in nodes:
            if dotfileskip is True and basename(node).startswith(".") is True:
                continue
            if target in ("f", "file", "files") and isfile(node) is False:
                continue
            elif target in ("d","dir","directory","directories") and isdir(node) is False:
                continue
            yield normpath(node)
    else:
        for node in nodes:
            for root,dirs,files in os.walk(node):
                if target in ("f", "file", "files"):
                    targets = files
                elif target in ("d","dir","directory","directories"):
                    targets = dirs
                else:
                    targets = files + dirs
                for tar in targets:
                    if dotfileskip is True and tar.startswith(".") is False:
                        yield normpath(pathjoin(root,tar))

if __name__ == "__main__":
    testdir = dirname(__file__) + "/../testdata/"
    def _testlsdir():
        assert list(lsdir(testdir + ".*",False,"d",False)) == []
        assert list(lsdir(testdir +"*",False,"f",False)) != []
        assert list(lsdir(testdir +"*")) != []
    _testlsdir()

