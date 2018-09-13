#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""
__version__ = "0.2.0"
__author__ = "m.yama"

from util.dfutil import read_any
from util.core import isdataframe

from itertools import combinations

import sys
import os
from io import StringIO

# 3rd party modules
import numpy as np
import pandas as pd


__all__ = [
        "profiler",
        "Profile",
        ]


def profiler(f, top=10, header=True, outputencoding="cp932", *args, **kw):
    df = read_any(f, *args, **kw)
    sio = StringIO()

    if isdataframe(df):
        df = [["-", df]]

    i = 0
    for k, cdf in (df.items() if isinstance(df, dict) else df):
        cdf = Profile(cdf, top=top).data
        cdf.insert(0, "sheetname_or_table", k)
        cdf.insert(0, "filename", f)
        cdf.to_csv(sio, index=False, sep=",", header=i == 0 and header, encoding=outputencoding)
        i += 1
    sio.seek(0)
    return sio.getvalue()


class Profile(object):
    def __init__(self, df, top=10, percentile=95):
        self.df = df if (df.dtypes == object).all() else df.astype("str")
        self._top = top
        self.top = top
        self._profiler = None
        self._guesskey = None
        self._diffkey = None
        self._percentile = percentile
        self.percentile = percentile

    @property
    def data(self):
        if self._profiler is None:
            exctop = (self.df[c].value_counts().head(self.top).index.tolist() for c in self.df.columns)
            if hasattr(self.df, "compute"):
                rec = self.df.index.size.compute()
                pr = pd.DataFrame(self.df.count().compute().rename("count"))
                pr["unique"] = pd.DataFrame([self.df[c].nunique().compute() for c in self.df.columns], index=self.df.columns)
                if self.top and self.top > 0:
                    pr["top"] = list(exctop)
            else:
                rec = self.df.index.size
                pr = self.df.describe().T
                if self.top and self.top > 1:
                    pr["top"] = list(exctop)

            pr.reset_index(inplace=True)
            pr.rename(columns=dict(index="cols"),inplace=True)
            pr["rec"] = rec
            pr["uniqrate"] = (pr["unique"] / rec)
            pr["valrate"] = (pr["count"] / rec)
            pr["keyrate"] = (pr["uniqrate"] * pr["valrate"])
            self._profiler = pr[['cols','rec','count','unique','uniqrate','valrate','keyrate','top']]
        return self._profiler

    @property
    def guesskey(self):
        if self._guesskey is None or self._top != self.top:
            pr = self.data.sort_values("keyrate", ascending=False)
            self._guesskey = pr.head(self.top)[["cols", "keyrate"]]
        return self._guesskey

    @property
    def diffkey(self):
        if self._diffkey is None or self._percentile != self.percentile:
            key = self.guesskey

            kg = []
            pt = []
            for i in range(len(key), 0, -1):
                for cb in combinations(key.cols, i):
                    skew = sum(key.keyrate[key.cols == c].sum() for c in cb)
                    kg.append((skew, list(cb)))
                    pt.append(skew)
            kg.sort(reverse=True)
            percentile = np.percentile(pt, self.percentile)
            self._diffkey = dict(("diff{:02d}".format(i+1),k) for i, (s, k) in enumerate(kg) if s > percentile or len(k) == len(key.cols))
        return self._diffkey
    def __getitem__(self, name):
        k = "diff{:02d}".format(int(name.split("diff")[-1]))
        return self.diffkey[k]
    def __getattr__(self, name):
        return self.__getitem__(name)


def main():
    from argparse import ArgumentParser
    from glob import glob

    usage="""
    Any file Data Profiler
       Example1: {0} [-v] [-n 30] [--encoding utf-8] "C:\\hoge\\test.csv"
       Example2: {0} *.xlsx

    """.format(os.path.basename(sys.argv[0]).replace(".py", ""))

    ps = ArgumentParser(usage)
    padd = ps.add_argument

    padd("-v", "--verbose",
         help="処理中のファイル名を表示する",
         action='store_true', default=False)
    padd("-n", "--numtop",
         help="TOPn件のサンプルデータを表示する(デフォルトtop10)",
         type=int, default=10)
    padd("--encoding",
         help="input file encoding default auto",
         default=None)
    padd("--outputencoding",
         help="出力ファイルのエンコーディングを指定する(デフォルトはcp932)\n選択肢cp932 utf-8 eucjp",
         default="cp932")
    padd("filename",
         metavar="<filename>",
         nargs="+",  default=[],
         help="target any files(.txt, .csv, .tsv, .xls[x], .accdb, .sqlite3)")
    args = ps.parse_args()

    i = 0
    for arg in args.filename:
        for f in glob(arg):
            f = os.path.normpath(f)
            if args.verbose:
                sys.stderr.write("Profiling:{}\n".format(f))
                sys.stderr.flush()
            print(profiler(f, args.numtop, i == 0, outputencoding=args.outputencoding))
            i += 1

    if i == 0:
        raise ValueError("ファイルが見つかりませんでした\n\n" + usage)


"""
   TestCase below
"""

def test():
    from util.core import tdir
    def test_profiler():
        f = tdir + "diff1.csv"
        df = read_any(f).head(3)
        pr = Profile(df)
        print(pr.data.to_csv(index=False))
        print(profiler(f, top=None))

    #TODO
    def test_Profile():
        pass

    def test_main():
        sys.argv.extend(["-v", tdir + "sample.accdb"])
        main()


    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            func()


def make_test_src_print(
        srctmpl = "\n    #TODO\n    def test_{}():\n        pass\n",
        exclude = ["make_test_src_print", "make_all_src_print",
                   "test", "main", "test_main"]):

    import types

    for k, v in dict(globals()).items():
        src = srctmpl.format(k)
        if k in exclude:
            continue
        if isinstance(v, type) and v.__module__ == "__main__":
            print(src)
        if isinstance(v, types.FunctionType) and v.__module__ == "__main__":
            print(src)


if __name__ == "__main__":
    # test()
    main()


