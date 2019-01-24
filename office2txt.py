#!/usr/sbin/env python3
# -*- coding: utf-8 -*-
import sys, re, traceback
from util import xdoc, lsdir, opener

incread_files = re.compile("\.(xls|doc|ppt|pdf)")

def render(t, sep="\t", sep2=" "):
    """
    sep : separator of filename, line, data
    sep2 : separator of datalist( ex. excel cell rows)
    """
    if isinstance(t[2], (list,tuple)):
        return sep.join([str(t[0]), str(t[1]), sep2.join(map(str,t[2]))])
    else:
        return sep.join(map(str,t))

def reader_all(path):
    for p in lsdir(path):
        if incread_files.search(p.ext):
            try:
                for t in xdoc.any(p):
                    print(render(t))
    
            except:
                sys.stderr.write("Error: {}\n".format(p))
                traceback.print_exc()

        else:
            if p.encoding is None:
                print(render([path,"binaryfile",""]))
            else:
                with open(path, "r", encoding=p.encoding) as f:
                    for i, line in enumerate(f, 1):
                        print(render([path,i,line.strip()]))

if __name__ == "__main__":
    import os
    
    if len(sys.argv) > 1:
        for dp in sys.argv[1:]:
            reader_all(dp)
    else:
        sys.stderr.write("Usage: {} [Office Files] ... \nWild Card OK".format(os.path.basename(sys.argv[0])))
