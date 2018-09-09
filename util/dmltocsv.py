#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import re
import codecs
import csv
from glob import glob


def dml_tolist( buffer,
                excludes = ["^;?\n", "^;$", "^\s*//.*$"],
                lvsep = "  "):
    """
    Return: [linenum, src, record_names, field_info]
    """
    ret = []

    lines = list(enumerate(buffer.readlines(),1))
    maxlen = max(x.count(lvsep) for i,x in lines)

    rec = [""] * maxlen

    for i, line in reversed(lines):

        if line and all(not re.match(x, line) for x in excludes):
            row = line.rstrip().split(lvsep)
            name = row[-1]
            if name.startswith("end"):
                recordname = name.replace("end", "").strip("[; ]")
                rec[row.index(name)] = recordname
            elif name.startswith("record"):
                pass
            else:
                dmlfield = re.split("(?:\(|\)\s+)", name.split(";")[0])
                ret.append([i, line.rstrip(), rec, dmlfield])

    ret.reverse()
    return ret

def filemain():
    ret = []
    maxlen = 0
    for x in sys.argv[1:]:
        for g in glob(x):
            with codecs.open(g, encoding="utf-8") as f:
                for i, line, rec, field in dml_tolist(f):
                    if maxlen < len(rec):
                        maxlen = len(rec)
                    ret.append([f.name, i, *rec, field[-1], line.strip()])

    w = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)

    for r in ret:
        row = r[:2] + r[2:-2] + [""] * (maxlen - len(r[2:-2])) + r[-2:]
        w.writerow(row)

def stdinmain():
    w = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
    for i, line, rec, field in dml_tolist(sys.stdin):
        w.writerow(["NoName", i, *rec, field[-1], line.strip()])

if __name__ == "__main__":
    if len(sys.argv) > 1:
        filemain()
    elif not sys.stdin.isatty():
        stdinmain()
    else:
        sys.stderr.write("Usage1: {0} utf8_*.dml\nUsage2: cat hoge.dml | {0}\n".format(sys.argv[0]))
        sys.exit(1)