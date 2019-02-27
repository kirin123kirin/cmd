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
import pandas as pd
import dask.dataframe as dd


__all__ = [
        "profiler",
        "Profile",
        ]

def skew(key):
    for i in range(1, len(key)):
        for x in combinations(key, i):
            yield sum(key[k] for k in x), x

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
    dnrule = "diff{:02d}".format

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
            cols = self.df.columns.tolist()
            rec = len(self.df)
            if cols == [] and rec == 0:
                return pd.DataFrame(columns=['cols','rec','count','unique','uniqrate','valrate','keyrate','top'])

            if hasattr(self.df, "compute"):
                pr = pd.concat([
                        self.df.count().rename("count").compute(),
                        self.df.nunique().rename("unique").compute()], axis=1)

            else:
                pr = pd.concat([
                        self.df.count().rename("count"),
                        self.df.nunique().rename("unique")], axis=1)

            if self.top:
                pr["top"] = list(self.df[c].value_counts().head(self.top).index.tolist() for c in cols)
            else:
                pr["top"] = list(self.df[c].value_counts().index.tolist() for c in cols)

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
            dt = self.data.loc[self.data.keyrate > 0, ["cols", "keyrate"]]
            self._guesskey = dt.set_index("cols").keyrate.to_dict()
        return self._guesskey

    @property
    def diffkey(self):
        if self._diffkey is None or self._percentile != self.percentile:
            df = pd.DataFrame(skew(self.guesskey), columns=["skw", "key"])
            pt = df.skw.quantile(self.percentile / 100.0)
            df = df[(df.skw > pt) | (df.key.apply(len) == len(self.guesskey)-1)].sort_values("skw", ascending=False)
            df.index = list(map(self.dnrule, range(1, df.index.size + 1)))

            self._diffkey = df.key.to_dict()
        return self._diffkey

    def __getitem__(self, name):
        k = self.dnrule(int(name.split("diff")[-1]))
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
    from datetime import datetime as dt

    def test_Profiler():
        f = tdir + "diff1.csv"
        df = read_any(f).head(3)
        pr = Profile(df)
        print(pr.data.to_csv(index=False))

    def test_profiler():
        f = tdir + "diff1.csv"
        print(profiler(f, top=None))


    def test_diffkey_pandas():
        f = tdir + "diff1.csv"
        df = read_any(f).head(3)
        t1 = dt.now()
        pr = Profile(df)
        pr.diffkey
        t2 = dt.now()
        print("diffkey_pandas", t2-t1)

    def test_diffkey_dask():
        f = tdir + "diff1.csv"
        df = dd.from_pandas(read_any(f).head(3),1)
        t1 = dt.now()
        pr = Profile(df)
        pr.diffkey
        t2 = dt.now()
        print("diffkey_dask", t2-t1)

    def test_main():
        sys.argv.extend(["-v", tdir + "sample.accdb"])
        main()

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = dt.now()
            func()
            t2 = dt.now()
            print("{} : time {}".format(x, t2-t1))


if __name__ == "__main__":
#    test()
    main()


