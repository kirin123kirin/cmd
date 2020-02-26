#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

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
    "tdir",
    "TMPDIR",
]

isposix = os.name == "posix"
iswin = os.name == "nt"

import re
from io import IOBase, StringIO, BytesIO
import codecs
from os.path import isdir
from glob import iglob
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

def flatten(x):
    return [z for y in x for z in (flatten(y)
             if y is not None and hasattr(y, "__iter__") and not isinstance(y, (str, bytes, bytearray)) else (y,))]

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
        if isinstance(f, StringIO):
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


def globbing(func, ttype="both", callback=None):
    """ globbable function decorator

         Parameters:
             func : callable => decorate target function object
                * `func` Require: 1st Argument is need path_or_buffer
             ttype : str         => both or file or dir
             key : callable   => finalize output function

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
        if isinstance(path_or_buffer, (str, bytes)):
            if len(path_or_buffer) > 1023:
                raise ValueError("Not Valid file path string.")
            elif any(x in path_or_buffer for x in "*?["):
                tp = (ttype or "b").lower()[0]
                if tp == "b":
                    it = (dat for x in iglob(path_or_buffer) for dat in func(x, *args, **kw))
                elif tp == "d":
                    it = (dat for x in iglob(path_or_buffer) if isdir(x) for dat in func(x, *args, **kw))
                elif tp == "f":
                    it = (dat for x in iglob(path_or_buffer) if not isdir(x) for dat in func(x, *args, **kw))
                else:
                    raise ValueError("Unknown ttype `both`, `file`, `dir`")
            else:
                it = func(path_or_buffer, *args, **kw)

        elif isinstance(path_or_buffer, (list, tuple)) or hasattr(path_or_buffer, "__next__"):
            it = (dat for x in path_or_buffer for dat in func(x, *args, **kw))

        else:
            it = func(path_or_buffer, *args, **kw)

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

    wrapper.__doc__ = func.__doc__
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

def translates(repdic, string):
    pattern = '({})'.format('|'.join(map(re.escape, repdic.keys())))
    return re.sub(pattern, lambda m: repdic[m.group()], string)

from socket import gethostname
from tempfile import TemporaryDirectory

__tdpath = "/portable.app/usr/share/testdata/"
if gethostname() == "localhost":
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


