# -*- coding: utf-8 -*-
import sys
import os
import codecs
import pandas as pd

def uniq_pop(df):
    define = [7,7,8,8,8,13,8,8,8,8] # 組織コード1から10のバイト数 組織2は実際データが7バイトのため
    df.drop_duplicates(inplace=True) #一応ユニークに
    for i, d in zip(reversed(range(1,10)), define): #9から1階層を増殖してユニークに
        r = pd.concat([df["組織コード"].str.slice(0,sum(define[:i])), df[['グループコード', '汎用コード']]], axis=1)
        r.drop_duplicates(inplace=True) #再びユニークにする
        r.insert(0, "階層", i) #階層番号を先頭カラムにぶち込んでる
        r["登録日時"] = "" #空っぽでいんやろか?
        r["更新日時"] = "" #空っぽでいんやろか?
        yield r #てな感じに1階層ごとにブンまわす


if __name__ == "__main__":
    import argparse
    from tempfile import gettempdir
    if len(sys.argv) == 1: #引数なければクリップボードから
        df = pd.read_clipboard(dtype="object")
        outpath = os.path.join(gettempdir(), "組織グループマスタ.csv")
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('filename',
                            metavar='<filename>',
                            nargs=1,
                            help='Target File')
        parser.add_argument('-o',
                            '--outputfile',
                            help='outputfile path: default is Same Directory write',
                            default="組織グループマスタ.csv")
    
        args = parser.parse_args()
        inp = args.filename[0]
        outpath=os.path.join(os.path.dirname(inp), args.outputfile)
        ext = os.path.splitext(inp)[-1].lower()
        if ext.startswith(".xls"):
            df = pd.read_excel(inp, dtype="object")
        else:
            with codecs.open(inp, encoding="cp932") as f:
                df = pd.read_csv(f, dtype="object")

    ret = pd.concat(uniq_pop(df), ignore_index=True)
    ret.to_csv(outpath, index=False,
    #                   line_terminator="\r\n",
                       encoding="cp932")
    if len(sys.argv) == 1:
        os.system("start " + outpath)
        sys.exit(0)
