# -*- coding: utf-8 -*-
# author : m.yamagami
# require: pandas, dask, tlz, toolz, cloudpickle
# License: MIT
# copyright: m.yamagami@2017
import pandas as pd
import dask.dataframe as dd
import numpy as np
from six import string_types


import sys,os,re,codecs,io,shutil,traceback
from glob import glob
import psutil as ps
import zipfile, gzip, bz2, lzma, tarfile
import tempfile
from io import IOBase
import csv
from xlrd import XLRDError

enc        = "cp932"
verbose    = False
n_top      = 10

NULL_LIST  = [np.nan, "", "NULL"]
re_decimal = re.compile(r'^[0-9. ]*$')
re_ascii   = re.compile(r'^[!-~ ]*$')

#sys.stdin  = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
#sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="ignore")
#sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="ignore")

ZIPLIST    = [".zip", ".tgz", ".tar.gz", ".tar.bz", ".tar.bz2", ".tar.xz",
              ".gz", ".bz", ".bz2", ".gzip", ".xz", "lzma", ".tar"]

def logger(msg):
    if verbose:
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()

def memsize():
    import types
    print("{}{: >15}{}{: >10}{}".format('|','Variable Name','|','  Size','|'))
    print(" -------------------------- ")
    for k, v in globals().items():
        if hasattr(v, 'size') and not k.startswith('_') and not isinstance(v,types.ModuleType):
            print("{}{: >15}{}{: >10}{}".format('|',k,'|',str(v.size),'|'))
        elif hasattr(v, '__len__') and not k.startswith('_') and not isinstance(v,types.ModuleType):
            print("{}{: >15}{}{: >10}{}".format('|',k,'|',str(len(v)),'|'))

def outputter(msg):
    writer = sys.stdout
    writer.write(msg)
    writer.flush()

def unzip(filename):
    with zipfile.ZipFile(filename, "r") as zf:
        wdir = tempfile.mkdtemp()
        zf.extractall(path=wdir)
        return list(glob(os.path.join(wdir, "*")))

def ungz(filename):
    with gzip.GzipFile(filename, "rb") as zf:
        wdir = tempfile.mkdtemp()
        exname = re.sub("\.(gzip|gz)$","",filename)
        outpath = os.path.join(wdir,os.path.basename(exname))
        with open(outpath,"wb") as ex:
            ex.write(zf.read())
            return [outpath]
ungzip = ungz

def unbz2(filename):
    with bz2.BZ2File(filename, "rb") as zf:
        wdir = tempfile.mkdtemp()
        exname = re.sub("\.(bz|bz2)$","",filename)
        outpath = os.path.join(wdir,os.path.basename(exname))
        with open(outpath,"wb") as ex:
            ex.write(zf.read())
            return [outpath]

unbz = unbz2

def unxz(filename):
    with lzma.LZMAFile(filename, "rb") as zf:
        wdir = tempfile.mkdtemp()
        exname = re.sub("\.(xz|lzma)$","",filename)
        outpath = os.path.join(wdir,os.path.basename(exname))
        with open(outpath,"wb") as ex:
            ex.write(zf.read())
            return [outpath]
unlzma = unxz

def untar(filename, mode='r:*'):
    with tarfile.open(filename, mode=mode) as zf:
        wdir = tempfile.mkdtemp()
        zf.extractall(path=wdir)
        ret = []
        for (root, d, files) in os.walk(wdir):
            for fn in files:
                ret.append(os.path.join(root,fn))
        return ret

def untgz(filename):
    return untar(filename, mode='r:gz')
untargz = untgz

def untarbz2(filename):
    return untar(filename, mode='r:bz2')
untarbz = untarbz2

def untarxz(filename):
    return untar(filename, mode='r:xz')

def extract(filename):
    for ext in ZIPLIST:
        if ext in filename:
            logger("[INFO]: Extract to TMP directory from %s" % filename)
            return eval("un%s(filename)" % ext.replace(".",""))
    raise AttributeError

def getfilename(fp):
    if hasattr(fp, "name"):
        return fp.name
    else:
        return fp

def normext(filepath_or_buffer):
    f = getfilename(filepath_or_buffer)
    
    ext = os.path.splitext(f)[1].lower()
    
    if ext in (".gz", ".bz", ".bz2", ".gzip", ".xz"):
        return os.path.splitext(re.sub("\.(gzip|gz|bz|bz2|xz)$","",f))[1]
    elif ext == ".zip":
        with zipfile.ZipFile(f, 'r') as z:
            return os.path.splitext(z.infolist()[0].filename)[1]
    else:
        return ext

def guess_delimiter(filepath_or_buffer, encoding=enc):
    s = csv.Sniffer()
    if ".zip" in getfilename(filepath_or_buffer).lower():
        with zipfile.ZipFile(getfilename(filepath_or_buffer),"r") as z:
            with z.open(z.filelist[0].filename) as zf:
                return s.sniff(next(zf).decode(enc, "ignore")).delimiter
    elif isinstance(filepath_or_buffer, string_types):
        with codecs.open(filepath_or_buffer, encoding=encoding, errors="ignore") as f:
            return s.sniff(next(f)).delimiter
    elif isinstance(filepath_or_buffer, IOBase):
        orgtel = filepath_or_buffer.tell()
        ret = s.sniff(next(filepath_or_buffer)).delimiter
        filepath_or_buffer.seek(orgtel)
        return ret
    else:
        raise AttributeError

def dask_read_any(filepath_or_buffer, dtype="category",keep_default_na=False, encoding=enc,**kw):
    """ メモリに乗らない場合はこちらできる限り拡張子から読み方推測して読むぜ
    dask.dataframe read_csv, read_table, read_excelのラッパー
    Return:
        dask.dataframe object
    """
    fn = getfilename(filepath_or_buffer)
    orgext = os.path.splitext(fn)[-1].lower()
    ext = normext(filepath_or_buffer).lower() # zipの場合は中身の拡張子を取得
    args = dict(dtype=dtype, keep_default_na=keep_default_na, **kw)
    if orgext == ".zip":
    	raise RuntimeError("解凍後のファイルでお願いします。")
    if orgext in ZIPLIST:
    	args = dict(compression=orgext[1:], **args)
    if ext.startswith(".xls"):
        try:
            return dd.read_excel(fn, **args)
        except XLRDError:
            raise XLRDError("Excelのパスワード保護を解除してから行ってください")
    elif ext == ".json":
        return dd.read_json(filepath_or_buffer, encoding=encoding, **args)
    else:
        d = guess_delimiter(fn)
        try:
            return dd.read_csv(filepath_or_buffer, sep=d, encoding=encoding, **args)
        except:
            return dd.read_csv(fn, sep=d, **args)

def pandas_read_any(filepath_or_buffer, dtype="category",keep_default_na=False, encoding=enc,**kw):
    """ メモりに載るならpandasの通常読み込みできる限り拡張子から読み方推測して読むぜ
    pandas read_csv, read_table, read_excelのラッパー
    Return:
        pandas.DataFrame object
    """
    fn = getfilename(filepath_or_buffer)
    ext = normext(filepath_or_buffer).lower() # zipの場合は中身の拡張子を取得
    args = dict(dtype=dtype, keep_default_na=keep_default_na, **kw)
    if ext.startswith(".xls"):
        try:
            return pd.read_excel(fn, **args)
        except XLRDError:
            raise XLRDError("Excelのパスワード保護を解除してから行ってください")
    elif ext == ".json":
        return pd.read_json(filepath_or_buffer, encoding=encoding, **args)
    else:
        d = guess_delimiter(fn)
        try:
            return pd.read_csv(filepath_or_buffer, sep=d, encoding=encoding, **args)
        except UnicodeDecodeError:
            return pd.read_csv(fn, sep=d, **args)
        except OSError:
            return pd.read_csv(codecs.open(fn, encoding=encoding), sep=d, **args)

def is_needs_dask(f):
    # メモリの空き容量みて足りそうならpandasで一気に、
    # 無理そうならコツコツとdaskで回す
    root, ext = os.path.splitext(f)
    virtual_memory = ps.virtual_memory()
    available_memory_bytes = virtual_memory.available
    needs_memory = os.stat(f).st_size * 5
    if ext in ZIPLIST and  needs_memory * 10 > available_memory_bytes:
        return True
    if ext == ".zip":
        with zipfile.ZipFile(f, "r") as zf:
            return zf.getinfo(zf.namelist()[0]).file_size * 5 > available_memory_bytes
    elif ".xls" in ext:
    	return False
    return available_memory_bytes < needs_memory

def read_any(f, encoding=enc, **kw):
    if is_needs_dask(f):
        if os.path.splitext(f)[1] in ZIPLIST:
            f = extract(f).pop(0) #TODO 複数ファイルある場合が未対応
        ret = dask_read_any(f,encoding=encoding, **kw)
        ret.todoclean = os.path.dirname(f)
    else:
        ret = pandas_read_any(f,encoding=enc, **kw)
        ret.todoclean = None
    return ret
            
def pandas_profile(df, top=10):
    n_rec = len(df)
    logger("[DEBUG] analyzing: {:,} line * {:,} columns\n".format(n_rec,len(df.columns)))
    for col in df.columns:
        xdf = pd.DataFrame(index=[col])
        xdf["カラム名"]             = col
        xdf["レコード数"]           = n_rec
        
        ds = df[col].str.encode(enc).str.len().describe()
        xdf["最大バイト長"]         = int(ds["max"])
        xdf["最小バイト長"]         = int(ds["min"])

        n_null = df[col].isin(NULL_LIST).sum()
        xdf["NULL数"]               = n_null
        xdf["NOTNULL判定"]          = n_null == 0
        xdf["ユニーク数"]           = df[col].nunique()
        xdf["ユニーク判定"]         = df[col].is_unique

        isascii = df[col].str.match(re_ascii).all()
        xdf["ALL_ASCII判定"]        = isascii
        xdf["ALL_数値型判定"]       = df[col].str.match(re_decimal).all()
        xdf["ALL_マルチバイト判定"] = not isascii
        xdf["TOP%d値" % top]        = str(df[col].value_counts().head(top).index.tolist())

        yield xdf

def dask_profile(df, top=10):
    n_rec = len(df)
    logger("[DEBUG] analyzing: {:,} line * {:,} columns\n".format(n_rec,len(df.columns)))
    for col in df.columns:
        xdf = pd.DataFrame(index=[col])
        xdf["カラム名"]             = col
        xdf["レコード数"]           = n_rec

        ds = df[col].str.encode(enc).str.len().describe().compute()
        xdf["最大バイト長"]         = int(ds["max"])
        xdf["最小バイト長"]         = int(ds["min"])

        n_null = df[col].isin(NULL_LIST).sum().compute()
        xdf["NULL数"]               = n_null
        xdf["NOTNULL判定"]          = n_null == 0

        n_uniq = df[col].nunique().compute()
        xdf["ユニーク数"]           = n_uniq
        xdf["ユニーク判定"]         = n_rec == n_uniq

        isascii = df[col].str.match(re_ascii).all()
        xdf["ALL_ASCII判定"]        = isascii
        xdf["ALL_数値型判定"]       = df[col].str.match(re_decimal).all()
        xdf["ALL_マルチバイト判定"] = not isascii
        xdf["TOP%d値" % top]        = str(df[col].value_counts().nlargest(top).compute().index.tolist())

        yield xdf

def profile(df, top=10):
    module = df.__module__.split(".")[0].lower()
    if module == "pandas":
        return pandas_profile(df, top)
    elif module == "dask":
        return dask_profile(df, top)
    else:
        raise RuntimeError("Unknown DataFrame")
 
def main(f, top=n_top, header=True):
    filesize = "{:,.1f}KB".format(os.stat(f).st_size / 1024)
    logger("[DEBUG] loading: %s (Size %s)" % (os.path.basename(f) ,filesize))
    try:
        df = read_any(f, encoding=enc, dtype="object")
        pf = profile(df, top)
        for idx, cdf in enumerate(pf):
            cdf.insert(0, "ファイル名", f)
            outputter(cdf.to_csv(index=False, header= header and idx == 0, encoding=enc))
    except:
        traceback.print_exc()
    finally:
        try:
            if df.todoclean:
                shutil.rmtree(df.todoclean)
            del df
            del pf
        except:
            pass

if __name__ == '__main__':
    import optparse

    usage = "usage: python %prog [-v] [-n 3] [-e utf-8] ターゲットファイルパス [...] > output.csv\n"
    op = optparse.OptionParser(usage)

    op.add_option("-v", "--verbose", action="store_true", default=False,
                            help="進捗状況など詳細表示する")
    op.add_option("-n", "--numtop",action="store", type="int", default=10,
                            help="出力項目のTOPn値について上位何位のデータサンプルをとるかどうか数を設定(デフォルトはtop10)")
    op.add_option("-e", "--encoding", type='choice', choices=['cp932', 'utf-8', 'eucjp'], default="cp932",
                            help="読み込みファイルのエンコーディングを指定(デフォルトはcp932)\n選択肢はcp932 utf-8 eucjp")

    options, args = op.parse_args()

    if not args:
        sys.stderr.write("ターゲットファイルを入れてください\n\n" + op.usage)
        sys.exit(1)

    enc         = options.encoding
    verbose = options.verbose
    n_top     = options.numtop

    argsL = []
    for arg in args:
        argsL.extend([os.path.normpath(a) for a in glob(arg)])
    i = 0
    if isinstance(argsL, list):
        for arg in argsL:
            main(arg, n_top, i == 0)
            i += 1
    else:
        main(argsL, n_top)
        i = 1
  
    if i == 0:
        sys.stderr.write("ターゲットファイルが一つも見つからず空振りました。\n\n" + op.usage)
        sys.exit(1)

