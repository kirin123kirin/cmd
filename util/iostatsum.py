#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import re
import codecs
from glob import glob
from datetime import datetime
from collections import defaultdict

def getiostat(filepath, encoding="utf-8"):
    with codecs.open(filepath, encoding=encoding) as f:
        if sys.version_info[0] == 2:
            dat = f.read().encode(encoding)
        else:
            dat = f.read()
        return re.split(r"\n\n(?=[12]\d{3})", dat, re.DOTALL)

resp = re.compile(r"\n\n")
table = re.compile(r"[ \t]+")
def devices_summary(data, filterdev=None):
    a, b = resp.split(data.rstrip())
    timestr = a.splitlines()[0]
    devices = (table.split(s) for s in b.splitlines())

    devhead = next(devices)
    devidx = {i:0.0 for i in range(1,len(devhead))}

    num = len(devhead)
    if filterdev:
        fd = re.compile(filterdev)

    for dev in devices:
        if filterdev and not fd.search(dev[0]):
            continue
        for i in range(1,num):
            devidx[i] += float(dev[i])

    r = {devhead[i]: devidx[i] for i in range(1,num)}
    r["datetime"] = timestr
    return r

def convert_dt(timestr, informat,outformat):
    dt = datetime.strptime(timestr, informat)
    return dt.strftime(outformat)

def average(array):
	return sum(array) / len(array)

def parser_simple(filepath, header, indateform, outdateform):
    for dat in getiostat(filepath)[1:]:
        r = devices_summary(dat)
        r["datetime"] = convert_dt(r["datetime"], indateform, outdateform)
        yield [r[h] for h in header]

def parser_rollup(filepath, header, indateform, outdateform, callable=average):
    ret = defaultdict(lambda : defaultdict(list))
    if "datetime" in header:
        del header[header.index("datetime")]
    for dat in getiostat(filepath)[1:]:
        r = devices_summary(dat)
        k = convert_dt(r["datetime"], indateform, outdateform)
        for h in header:
            ret[k][h].append(r[h])

    for k in sorted(ret) if sys.version_info[0] == 2 else ret:
        y = [k]
        for h in header:
            y.append(callable(ret[k][h]))
        yield y

def main():
    from argparse import ArgumentParser
    p = ArgumentParser(description="main templace")
    padd = p.add_argument

    padd('-f', '--field',help='output target field of iostat log.   (choise: tps,kB_read/s,kB_wrtn/s,kB_read,kB_wrtn',
         default="datetime,tps,kB_read/s,kB_wrtn/s")

    padd('-s', '--separator',help='output field separate charcter  (ex. \\t)',
         default=",")

    padd('-I', '--inputdateform',help='iostat datetime format define  (ex. %%Y-%%m-%%d %%H:%%M:%%S)',
         default="%Y年%m月%d日 %H時%M分%S秒")

    padd('-O', '--outputdateform',help='print out datetime format define (ex. %%Y-%%m-%%d %%H:%%M:%%S)',
         default="%H:%M:%S")

    padd('-o', '--outputfile',help='output file default `stdout`',
         default=None)

    padd('-a', '--aggregatefunc',help='aggregate function name (ex. `average`, `sum`)  and groupby outputdateform setting',
         default=None)

    padd('filepath', nargs="+", help='Target iostat Log Files (WildCard Path OK)   (ex. hoge/*.log')
    args = p.parse_args()

    header=args.field.replace(" ", "").split(",")
    sep= args.separator
    files = args.filepath
    indateform = args.inputdateform
    outdateform = args.outputdateform
    if args.aggregatefunc is None:
        parser = parser_simple
    else:
        parser = lambda *k, **v: parser_rollup(*k, callable=globals()[args.aggregatefunc], **v)


    with  open(args.outputfile, "w") if args.outputfile else sys.stdout as w:
        i = 0
        for file in files:
            for filepath in glob(file):
                if i == 0:
                    w.write(sep.join(header) + "\n")
                for ret in parser(filepath, header, indateform, outdateform):
                    w.write(ret[0] + sep + sep.join(map("{:.3f}".format, ret[1:])) + "\n")
                    i += 1

def test():
    sys.argv.extend("-a average -O %H:%M -s \t iostat.log".split(" "))
    main()

if __name__ == "__main__":
#	test()
	main()