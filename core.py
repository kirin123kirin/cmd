#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys

__all__ = [
    "isposix",
    "iswin",
    "getencoding",
    "flatten",
    "binopen",
    "opener",
    "binchunk",
    "globbing",
    "to_hankaku",
    "to_zenkaku",
    "hostname",
    "getipaddr",
    "mkdirs",
    "move",
    "rdict",
    "re_call",
    "tdir",
    "TMPDIR",
]

isposix = os.name == "posix"
iswin = os.name == "nt"

import re
from io import IOBase, StringIO, BytesIO
import codecs
from os.path import isdir, normpath, abspath, dirname, basename, exists, join as pathjoin
from glob import glob, iglob
from functools import wraps
from socket import gethostname, gethostbyname
import shutil
from functools import lru_cache

try:
    import nkf
    def getencoding(dat:bytes):
        if b"\0" in dat:
            return None
        enc = nkf.guess(dat).lower()
        if enc and enc == "shift_jis":
            return "cp932"
        elif enc == "binary":
            return None
        else:
            return enc
except ModuleNotFoundError:
    try:
        import chardet
        def getencoding(dat:bytes):
            return chardet.detect(dat)["encoding"]
    except ModuleNotFoundError:
        raise ModuleNotFoundError("please install `nkf` or `chardet`")

def thisfile():
    return abspath(sys.argv[0])

def thisdir():
    return dirname(thisfile())

def flatten(x):
    return [z for y in x for z in (flatten(y)
             if y is not None and hasattr(y, "__iter__") and not isinstance(y, (str, bytes, bytearray)) else (y,))]

def listify(x):
    if not x:
        return []
    elif isinstance(x, list):
        return x
    elif isinstance(x, (str, bytes, bytearray, int, float, bool, )):
        return [x]
    elif hasattr(x, "__iter__") or hasattr(x, "__next__"):
        return list(x)
    else:
        return [x]


def binopen(f, mode="rb", *args, **kw):
    check = lambda *tp: isinstance(f, tp)

    if check(str) or hasattr(f, "joinpath"):
        return open(f, mode, *args, **kw)

    klass = f.__class__.__name__
    if check(BytesIO) or klass in ["ExFileObject", "ZipExtFile"]:
        return f

    if check(bytearray, bytes):
        bio = BytesIO(f)
        bio.name = None
        return bio

    if check(StringIO):
        e = f.encoding
        if e:
            bio = BytesIO(f.getvalue().encode(e))
        else:
            bio = BytesIO(f.getvalue().encode())
        bio.name = f.name if hasattr(f, "name") else None
        return bio

    try:
        m = f.mode
    except AttributeError:
        m = f._mode
    if isinstance(m, int) or "b" in m:
        return f
    else:
        return open(f.name, mode=m + "b")

    raise ValueError("Unknown Object `{}`. filename or filepointer buffer".format(type(f)))

def opener(f, mode="r", *args, **kw):
    if isinstance(f, IOBase):
        if isinstance(f, StringIO) or f in [sys.stdout, sys.stderr, sys.stdin]:
            return f
        elif isinstance(f, BytesIO):
            if "encoding" in kw:
                return StringIO(f.getvalue().decode(kw["encoding"]))
            else:
                p = f.tell()
                e = getencoding(f.read(92160))
                r = StringIO(f.getvalue().decode(e))
                f.seek(p)
                return r

        m = f.mode

        if "b" not in m:
            return f
        else:
            name = f.name
            p = f.tell()
            if not hasattr(f, "write") and hasattr(f, "read"):
                kw["encoding"] = getencoding(f.read(92160))
            kw["mode"] = m.replace("b", "")
            f.close()
            r = codecs.open(name , *args, **kw)
            r.seek(p)
            return r
    elif isinstance(f, str) or hasattr(f, "joinpath"):
        if not any(x in mode for x in "aw+"):
            with open(f, "rb") as fp:
                kw["encoding"] = getencoding(fp.read(92160))
        return codecs.open(f, mode=mode.replace("b", ""), *args, **kw)
    else:
        raise ValueError("Unknown Object. filename or filepointer buffer")

def binchunk(path_or_buffer, buffer=1024**2, sep=None):
    with binopen(path_or_buffer) as fp:
        prev = b""

        if sep:
            while True:
                ret = fp.read(buffer)
                if ret == b"":
                    break

                for r in BytesIO(ret.replace(sep, b"\n") + prev):
                    if r.endswith(b"\n"):
                        yield r
                    else:
                        prev = r
        else:
            while True:
                ret = fp.read(buffer)
                if ret == b"":
                    break

                for r in BytesIO(ret + prev):
                    if r.endswith("\n"):
                        yield r
                    else:
                        prev = r


def dynamic_args(func0):
    def wrapper(*args, **kwargs):
        if len(args) != 0 and callable(args[0]):
            func = args[0]
            return wraps(func)(func0(func))
        else:
            def _wrapper(func):
                return wraps(func)(func0(func, *args, **kwargs))
            return _wrapper
    return wrapper


@dynamic_args
def globbing(func, ttype="both", isloop=True, callback=None):
    """ globbable function decorator

          Parameters:
              func : callable => decorate target function object
                * `func` Require: 1st Argument is need path_or_buffer
              ttype : str         => both or file or dir
              isloop : boolean    => return of func loop iteration?
              callback : callable   => finalize output function

          Return:
              globbable function

          Examples:
              @globbing
              def foo(path_or_buffer, *args, **kw):
                  return codecs.open(path_or_buffer, *args, **kw)

              for line in foo("/tmp/*.csv", encoding="cp932"):
                  print(line)
    """
    def wrapper(path_or_buffer, *args, **kw):
        it = None
        if isinstance(path_or_buffer, (str, bytes)):
            if len(path_or_buffer) > 1023:
                raise ValueError("Not Valid file path string.")
            elif any(x in path_or_buffer for x in "*?["):
                tp = (ttype or "b").lower()[0]
                ig = map(normpath, iglob(path_or_buffer, recursive=True))

                if tp == "b":
                    if isloop:
                        it = (dat for x in ig for dat in func(x, *args, **kw))
                    else:
                        it = (func(x, *args, **kw) for x in ig)
                elif tp == "d":
                    if isloop:
                        it = (dat for x in ig if isdir(x) for dat in func(x, *args, **kw))
                    else:
                        it = (func(x, *args, **kw) for x in ig if isdir(x))
                elif tp == "f":
                    if isloop:
                        it = (dat for x in ig if not isdir(x) for dat in func(x, *args, **kw))
                    else:
                        it = (func(x, *args, **kw) for x in ig if not isdir(x))
                else:
                    raise ValueError("Unknown ttype `both`, `file`, `dir`")

        elif isinstance(path_or_buffer, (list, tuple)) or hasattr(path_or_buffer, "__next__"):
            it = (y for x in path_or_buffer for y in wrapper(x, *args, **kw))

        if it is None:
            if isloop:
                it = func(path_or_buffer, *args, **kw)
            else:
                it = [func(path_or_buffer, *args, **kw)]

        flag = False
        if callback:
            for item in it:
                yield callback(item)
                if flag is False:
                    flag = True
        else:
            for item in it:
                yield item
                if flag is False:
                    flag = True

        if flag is False:
            raise FileNotFoundError(path_or_buffer)

    if hasattr(func, "__doc__"):
        wrapper.__doc__ = func.__doc__
    if hasattr(func, "__name__"):
        wrapper.__name__ = func.__name__

    return wrapper



ZEN = "".join(chr(0xff01 + i) for i in range(94))
HAN = "".join(chr(0x21 + i) for i in range(94))

#thanks https://hgotoh.jp/wiki/doku.php/documents/other/other-020
#ZEN = """！＂＃＄％＆＇（）＊＋，－．／０１２３４５６７８９：；＜＝＞？＠ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ［＼］＾＿｀ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ｛｜｝～￠￡￢￤￥←↑→↓│■○"""
#HAN = """!"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]＾_`abcdefghijklmnopqrstuvwxyz{｜}~￠￡￢|\￩￪￫￬|￭￮"""

def to_hankaku(s):
    return s.translate(str.maketrans(ZEN, HAN))
def to_zenkaku(s):
    return s.translate(str.maketrans(HAN, ZEN))

def kanji2int(kstring: str, sep=False,
    tt_knum = str.maketrans('一二三四五六七八九〇壱弐参', '1234567890123'),
    re_num = re.compile(r'[十拾百千万億兆\d]+'),
    re_kunit = re.compile(r'[十拾百千]|\d+'),
    re_manshin = re.compile(r'[万億兆]|[^万億兆]+'),
    TRANSUNIT = {'十': 10,
                 '拾': 10,
                 '百': 100,
                 '千': 1000},
    TRANSMANS = {'万': 10000,
                 '億': 100000000,
                 '兆': 1000000000000}
    ):
    def _transvalue(sj: str, re_obj=re_kunit, transdic=TRANSUNIT):
        unit = 1
        result = 0
        for piece in reversed(re_obj.findall(sj)):
            if piece in transdic:
                if unit > 1:
                    result += unit
                unit = transdic[piece]
            else:
                val = int(piece) if piece.isdecimal() else _transvalue(piece)
                result += val * unit
                unit = 1

        if unit > 1:
            result += unit

        return result

    trannum = kstring.translate(tt_knum)
    for num in sorted(set(re_num.findall(trannum)), key=lambda s: len(s),
                           reverse=True):
        if not num.isdecimal():
            arabic = _transvalue(num, re_manshin, TRANSMANS)
            arabic = '{:,}'.format(arabic) if sep else str(arabic)
            trannum = trannum.replace(num, arabic)
        elif sep and len(num) > 3:
            trannum = trannum.replace(num, '{:,}'.format(int(num)))

    return trannum

def translates(repdic, string):
    pattern = '({})'.format('|'.join(map(re.escape, repdic.keys())))
    return re.sub(pattern, lambda m: repdic[m.group()], string)

hostname = gethostname

def getipaddr():
    return gethostbyname(hostname())

def gethost():
    return "{}({})".format(hostname(),getipaddr())

def mkdirs(tar):
    if not exists(tar):
        os.makedirs(tar)
        return True
    return False

def move(src, dst, makedirs=True):
    try:
        if exists(src):
            shutil.move(src, dst)
            return

        sources = glob(src, recursive=True)
        if not sources:
            raise OSError("Files not found `src path` is " + src)

        for s in sources:
            tar = pathjoin(dst, basename(s))
            if exists(tar):
                os.remove(tar)

            shutil.move(s, tar)
    except FileNotFoundError as e:
        if makedirs:
            mkdirs(dst)
            return move(src, dst)
        else:
            raise(e)

@lru_cache(16)
def re_call(pattern, flags=0, call_re_func="search"):
    return re.compile(pattern, flags=flags).__getattribute__(call_re_func)

class rdict(dict):
    """
    Summary
    ----------
        python辞書キーを正規表現でも探索できるよう拡張した辞書

    Example
    ----------
    >>> a = rdict()
    >>> a["abc"] = 1
    >>> a
    {'abc': 1}

    まず普通に辞書として使える

    >>> a["abc"]
    1

    キーを正規表現パターンで検索する

    >>> a.search(".b.*")
    [1]

    パターンが見つからない場合は空の配列を返す

    >>> a.search(".z.*")
    []

    正規表現パターンで一致するキーがあるかどうかを調べる

    >>> a.isin(re.compile(".b.*").search)
    True
    >>> a.isin(re.compile(".z.*").search)
    False

    その他１、条件が真になる場合のキーを持つ場合の値を返す

    >>> a.findall(lambda x: len(x) == 3)
    [1]

    findallの引数はcallableな関数ならば何でも良いので以下のような応用もできる
    その他２、キーを範囲で検索し、値を返す

    >>> from datetime import datetime
    >>> b = rdict()
    >>> b[datetime(2020,1,1)] = "2020/01/01"
    >>> b[datetime(2020,2,1)] = "2020/02/01"
    >>> b[datetime(2020,3,1)] = "2020/03/01"

    >>> def between_0131_0202(x):
    ...    return datetime(2020,1,31) < x and x < datetime(2020,2,2)
    >>> b.findall(between_0131_0202)
    ['2020/02/01']

    >>> def less_0401(x):
    ...    return x < datetime(2020, 4, 1)
    >>> b.isin(less_0401)
    True

    >>> def grater_0401(x):
    ...    return x > datetime(2020, 4, 1)
    >>> b.isin(grater_0401)
    False

    >>> b.findall(less_0401)
    ['2020/01/01', '2020/02/01', '2020/03/01']

    条件にマッチするキーの値を一括で変更する

    >>> b[less_0401] = "test"
    >>> b
    {datetime.datetime(2020, 1, 1, 0, 0): 'test',
     datetime.datetime(2020, 2, 1, 0, 0): 'test',
     datetime.datetime(2020, 3, 1, 0, 0): 'test'}

    条件にマッチするキーを一括で削除する

    >>> del b[between_0131_0202]
    >>> b
    {datetime.datetime(2020, 1, 1, 0, 0): 'test',
     datetime.datetime(2020, 3, 1, 0, 0): 'test'}

    """
    def _filter(self, _callable):
        return (k for k in self if _callable(k))

    def isin(self, key_or_function):
        """
        key_or_function が真を返すkeyが一つでも存在する場合、真を返します。
        一つも存在しない場合は偽を返します。
        """
        if callable(key_or_function):
            return any(True for _ in self._filter(key_or_function))
        return dict.__contains__(self, key_or_function)

    def findall(self, key_or_function):
        """
        function が真を返すkeyがあるvalueをリストで返します。
        一つも存在しない場合は空の配列を返します
        """
        if callable(key_or_function):
            return [dict.__getitem__(self, key) for key in self._filter(key_or_function)]
        return dict.__getitem__(self, key_or_function)

    def search(self, pattern, flags=0):
        """
        正規表現patternがマッチ(部分一致)するdict keyがあればそれらのvalueの配列を返します。
        一つもpattenにマッチしない場合は空の配列を返します。
        引数の意味は `re.compile <https://docs.python.org/ja/3/library/re.html#re.compile>`_ と等価です。
        """
        return [dict.__getitem__(self,key) for key in self if re_call(pattern, flags, "search")(key)]

    def fullmatch(self, pattern, flags=0):
        """
        正規表現patternがマッチ(完全一致)するdict keyがあればそれらのvalueの配列を返します。
        一つもpattenにマッチしない場合は空の配列を返します。
        引数の意味は `re.compile <https://docs.python.org/ja/3/library/re.html#re.compile>`_ と等価です。
        """
        return [dict.__getitem__(self,key) for key in self if re_call(pattern, flags, "fullmatch")(key)]

    def __setitem__(self, key_or_function, value):
        """
        function が真のkeyに対してvalueを設定します。
        一つも存在しない場合は何も行われません。
        """
        if callable(key_or_function):
            for key in self._filter(key_or_function):
                dict.__setitem__(self, key, value)
        else:
            return dict.__setitem__(self, key_or_function, value)

    def __delitem__(self, key_or_function):
        """
        function が真のkeyに対して辞書からキーと値を削除します。
        一つも存在しない場合は何も行われません。
        """
        if callable(key_or_function):
            for key in list(self._filter(key_or_function)):
                dict.__delitem__(self, key)
        else:
            return dict.__delitem__(self, key_or_function)

    def between(self, start, stop):
        """
        startよりも大きく、stopよりも小さいkeyがある場合、見つかったvalueの配列を返します。
        一つも存在しない場合は空の配列を返します。
        start, stopは keyと比較可能な値である必要があります。
        比較できない値が入力された場合はTypeErrorが発生します。
        """
        return [dict.__getitem__(self,key) for key in self if start < key and key < stop]

    def startswith(self, prefix, start=None, end=None):
        """
        prefixの文字を前方一致でkeyを探し、一致するkeyがある場合、見つかったvalueの配列を返します。
        一つも存在しない場合は空の配列を返します。
        引数の意味は `str.startswith <https://docs.python.org/ja/3/library/stdtypes.html#str.startswith>`_ と等価です。
        """
        return [dict.__getitem__(self,key) for key in self if key.startswith(prefix, start, end)]

    def endswith(self, suffix, start=None, end=None):
        """
        prefixの文字を前方一致でkeyを探し、一致するkeyがある場合、見つかったvalueの配列を返します。
        一つも存在しない場合は空の配列を返します。
        引数の意味は `str.endswith <https://docs.python.org/ja/3/library/stdtypes.html#str.endswith>`_ と等価です。
        """
        return [dict.__getitem__(self,key) for key in self if key.endswith(suffix, start, end)]

    def append(self, k, x):
        """
        dict keyのvalueに対して配列としてxを末尾に追加します。
        """
        if dict.__contains__(self, k):
            r = dict.__getitem__(self, k)
            if isinstance(r, list):
                r.append(x)
                dict.__setitem__(self, k, r)
            else:
                dict.__setitem__(self, k, [r, x])
        else:
            dict.__setitem__(self, k, [x])

    def extend(self, k, iterable):
        """
        dict keyのvalueに対して配列としてiterableを拡張します
        """
        if dict.__contains__(self, k):
            r = dict.__getitem__(self, k)
            if isinstance(r, list):
                r.extend(iterable)
                dict.__setitem__(self, k, r)
            else:
                dict.__setitem__(self, k, [r, *iterable])
        else:
            dict.__setitem__(self, k, iterable)

    def merge(self, other):
        """
        otherの辞書とマージします。
        マージする場合に同じdict keyがある場合、valueは配列として結合します。
        updateの場合は同じdict keyがある場合は上書きする点が違います。
        """
        for k, value in other.items():
            if isinstance(value, (list, tuple)):
                self.extend(k, value)
            else:
                self.append(k, value)

    def values_count(self):
        """
        dict key 毎の 値の件数をカウントした結果を返します。
        戻値は dictです。
        """
        return {k: len(v) if isinstance(v, list) else 1 for k, v in dict.items(self)}

    def iteritems(self):
        return ((k, v if isinstance(v, list) else [v]) for k, v in dict.items(self))

    def values_flatten(self):
        def flatten(x):
            return [z for y in x for z in (flatten(y)
             if y is not None and hasattr(y, "__iter__") and not isinstance(y, (str, bytes, bytearray)) else (y,))]
        return flatten(dict.values(self))

    def items_flatten(self):
        for k, v in dict.items(self):
            if isinstance(v, list):
                for vv in v:
                    yield k, vv
            else:
                yield k, v


from tempfile import TemporaryDirectory

__tdpath = "/portable.app/usr/share/testdata/"
if hostname() == "localhost":
    tdir = "/storage/emulated/0/Android/data/com.dropbox.android/files/u9335201/scratch" + __tdpath
elif os.name == "posix":
    tdir = os.getenv("HOME") + "/Dropbox/" + __tdpath
else:
    tdir = "Y:/usr/share/testdata/"


class _tmpdir(TemporaryDirectory):
    def __del__(self):
        self.cleanup()
_tmp = _tmpdir()
TMPDIR = _tmp.name


