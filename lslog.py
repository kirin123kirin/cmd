#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys
from glob import  glob
from datetime import datetime
from io import StringIO
from pathlib import Path
from dateutil.parser._parser import parser, parserinfo

class _jpinfo(parserinfo):
    WEEKDAYS = [
            ("Mon", "/曜日", "/曜", "Monday"),
            ("Tue", "火曜日", "火曜", "火", "Tuesday"),
            ("Wed", "水曜日", "水曜", "水", "Wednesday"),
            ("Thu", "木曜日", "木曜", "木", "Thursday"),
            ("Fri", "金曜日", "金曜", "金", "Friday"),
            ("Sat", "土曜日", "土曜", "土", "Saturday"),
            ("Sun", "日曜日", "日曜", "日", "Sunday")]
    HMS = [
            ("h", "時", "hour", "hours"),
            ("m", "分", "minute", "minutes"),
            ("s", "秒", "second", "seconds")]
    AMPM = [
            ("am", "ａｍ", "午前", "a"),
            ("pm", "ｐｍ", "午後", "p")]

def to_datetime(timestr, parserinfo=_jpinfo(), **kwargs):
    timestr = timestr.replace("年", "/").replace("月", "/")
    return parser(info=parserinfo).parse(timestr, **kwargs)

def parse_date(s,
        base_date = datetime.now(),
        future_date = True,
        callback = None):

    dt = to_datetime(s)

    if future_date is False:
        if not base_date:
            base_date = datetime.now()
        elif isinstance(base_date, str):
            base_date = to_datetime(base_date)

        if dt > base_date:
            dt = dt.replace(year=base_date.year - 1)

    return callback(dt) if callback else dt

def _split_date_time(dt):
    return dt.strftime("%Y/%m/%d"), dt.strftime("%H:%M:%S")


class lstab(object):
    def __init__(self, file, parent = "", base_date=""):
        if isinstance(file, (list, tuple)):
            self.file = sum(map(glob, file), [])
        else:
            self.file = glob(file)
        self.parent = parent
        self.base_date = base_date
        self.header = ["permission", "owner", "group", "size", "date", "time",
                       "extention", "basename", "link_path", "fullpath"]

        self.fp = (line for f in self.file for line in self.open(f))
        self.rundate = re.compile("^(?:\[[\d\-/:\. ]{8,36}\] )")
        self.exist_p = re.compile(self.rundate.pattern + "?[^\s].*:$").match
        self.exist_c = re.compile(self.rundate.pattern + "?[drwxtslc\-]{10} .*").match
        self.mod_sp = re.compile(" +")
        self.df = None
        self.ncol = 0

    def parse_ls(self, line, parent=""):
        if line.startswith("l"):
            p, _, o, g, size, *d, name, _, ln = self.mod_sp.split(line)
        else:
            ln = ""
            p, _, o, g, size, *d, name = self.mod_sp.split(line)

        dt = parse_date(" ".join(d), self.base_date, future_date=False, callback=_split_date_time)
        try:
            size = int(size)
        except:
            pass
        ret = [p, o, g, size, *dt, name.split(".")[-1] if "." in name else "", name]
        parent = parent or self.parent
        if not parent:
            return [*ret, ln]
        fullpath = parent.rstrip("/") + "/" + name
        self.ncol = max(fullpath.count("/"), self.ncol)
        dirname = (fullpath if p.startswith("d") else parent).strip("/")
        return [*ret, ln, fullpath, *map("/".__add__, dirname.split("/"))]

    def open(self, file, *args, **kw):
        if hasattr(file, "read"):
            return file
        if isinstance(file, (str, Path)) and os.path.exists(file):
            try:
                fp = open(file, encoding="utf-8", *args, **kw)
                fp.read(1)
            except UnicodeDecodeError:
                fp = open(file, encoding="cp932", *args, **kw)
                fp.read(1)
            fp.seek(0)
            return fp
        return StringIO(file)

    def __iter__(self):
        current = self.parent

        for line in self.fp:
            line = self.rundate.sub("", line)
            if not line:
                continue

            line = line.rstrip()

            if self.exist_c(line):
                yield self.parse_ls(line, current)
            elif self.exist_p(line):
                if self.parent:
                    current = self.parent + line.strip(" :").lstrip(".")
                else:
                    current = line.strip(" :")

    def to_csv(self, filepath_or_buffer, sep=',', index=False, quotechar='"', quoting=0, **kw):
        import csv
        fp = filepath_or_buffer if hasattr(filepath_or_buffer, "write") else open(filepath_or_buffer, "w")
        if fp == sys.stdout:
            kw["lineterminator"] = "\n"
            writer = csv.writer(fp, delimiter=sep, quotechar=quotechar, quoting=quoting, **kw)
            if self.header:
                writer.writerow(self.header + ["DIRS*"])
            for x in self.__iter__():
                writer.writerow(x)
                fp.flush()
        else:
            return self.to_pandas().to_csv(filepath_or_buffer, sep=sep, index=index, quotechar=quotechar, quoting=quoting, **kw)

    def to_excel(self, outfile):
        from xlsxwriter import Workbook

        with Workbook(outfile) as book:
            header_fmt = book.add_format(
                dict(border=True, align="center", bold=True))

            sheet = book.add_worksheet()

            for i, row in enumerate(self.__iter__(), 1):
                sheet.write_row(i, 0, row)

            if not sheet.dim_colmax:
                sys.stderr.write("No Data\n")
                return

            if self.header:
                dirs = map("DIR{:02d}".format, range(sheet.dim_colmax - 9))
                sheet.write_row(0, 0, [*self.header, *dirs], header_fmt)
                sheet.autofilter(0, 0, sheet.dim_rowmax, sheet.dim_colmax)

    def to_pandas(self):
        from pandas import DataFrame
        if self.df is None:
            self.df = DataFrame(self.__iter__())
            if len(self.df.columns) == 9:
                self.df.columns = self.header[:-1]
            else:
                self.df.columns = [*self.header, *map("DIR{:02d}".format, range(self.df.shape[1] - len(self.header)))]
        return self.df

    def to_html(self, buf=None, columns=None, col_space=None, header=True, index=False, **kw):
        return self.to_pandas().to_html(buf, columns, col_space, header, index, **kw)

    def to_json(self, path_or_buf=None, orient="records", **kw):
        return self.to_pandas().to_json(path_or_buf, orient=orient, **kw)

def main():
    from argparse import ArgumentParser
    from glob import glob

    usage="""
    parse from `ls -lR` log string
       Example1: {0} *.log -o fileslist.csv
       Example2: {0} *.log -o fileslist.xlsx

    """.format(os.path.basename(sys.argv[0]).replace(".py", ""))

    ps = ArgumentParser(usage)
    padd = ps.add_argument

    padd('-o', '--outfile', type=str, default=None,
         help='output filepath (default `stdout`)')
    padd('-b', '--basedate', type=str, default=None,
         help='run ls -lR Date')
    padd('-c', '--currentdirectory', type=str, default=None,
         help='run ls -lR Current Directory Path String')
    padd("filename",
         metavar="<filename>",
         nargs="+",  default=[],
         help="target mlocate.db files")
    args = ps.parse_args()

    outfile = args.outfile or sys.stdout
    BASE_DATE = args.basedate
    PARENT = args.currentdirectory

    files = [g for fn in args.filename for g in glob(fn)]
    if not files:
        raise RuntimeError("Not found files {}".format(args.filename))

    tb = lstab(files, parent=PARENT, base_date=BASE_DATE)
    ext = str(outfile).lower().rsplit(".", 1)[1]
    if ext.startswith("xls"):
        tb.to_excel(outfile)
    elif ext.startswith("htm"):
        tb.to_html(outfile)
    elif ext.startswith("json"):
        tb.to_json(outfile)
    else:
        tb.to_csv(outfile)


if __name__ == "__main__":
#    sys.argv.extend(r"C:/temp/lsdir.log -o C:/temp/test.xlsx".split(" "))
    sys.argv.append(r"C:/temp/lsdir.log")

    main()
