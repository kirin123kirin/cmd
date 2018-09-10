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
                for x in range(row.index(name), maxlen):
                    rec[x] = ""
            else:
                dmlfield = re.split("(?:\(|\)\s+)", name.split(";")[0])
                ret.append([i, line.rstrip(), rec.copy(), dmlfield])

    ret.reverse()
    return ret

def filemain(outfp=sys.stdout):
    ret = []
    maxlen = 0
    for x in sys.argv[1:]:
        for g in glob(x):
            with codecs.open(g, encoding="utf-8") as f:
                for i, line, rec, field in dml_tolist(f):
                    if maxlen < len(rec):
                        maxlen = len(rec)
                    ret.append([f.name, i, *rec, field[-1], line.strip()])

    w = csv.writer(outfp, quoting=csv.QUOTE_ALL)

    for r in ret:
        row = r[:2] + r[2:-2] + [""] * (maxlen - len(r[2:-2])) + r[-2:]
        w.writerow(row)

    if len(ret) == 0:
        raise ValueError("Not Found Files {}".format(sys.argv[1:]))

def stdinmain():
    w = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
    for i, line, rec, field in dml_tolist(sys.stdin):
        w.writerow(["NoName", i, *rec, field[-1], line.strip()])

def main():
    if len(sys.argv) > 1:
        filemain()
    elif not sys.stdin.isatty():
        stdinmain()
    else:
        sys.stderr.write("Usage1: {0} utf8_*.dml\nUsage2: cat hoge.dml | {0}\n".format(sys.argv[0]))
        sys.exit(1)

def test():
    from util.core import tdir
    from io import StringIO

    def test_filemain():
        data = tdir+"test.dml"
        sys.argv.append(data)

        sio = StringIO()
        filemain(sio)

        assert(sio.getvalue().replace("\r\n", "\n").strip() == '''"{0}","2","","","","","","foo1","string(7) foo1;"
"{0}","3","","","","","","foo2","string(9) foo2;"
"{0}","5","","bar1","","","","foo3","ebicdic string(9) foo3;"
"{0}","7","","bar1","H2","","","booon","string(7) booon;"
"{0}","10","","bar1","H2","H3","H4","string hcol","string hcol;"
"{0}","17","","bar2","boke","","","baan","string(7) baan;"
"{0}","20","","bar2","boke","h3","h4","string hcol","string hcol;"
'''.format(tdir+"test.dml").strip())

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            func()

if __name__ == "__main__":
    # test()
    main()