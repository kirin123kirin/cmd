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
    "tdir",
    "TMPDIR",
]

isposix = os.name == "posix"
iswin = os.name == "nt"

from io import IOBase, StringIO, BytesIO
import codecs
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
