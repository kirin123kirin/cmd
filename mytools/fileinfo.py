# -*- coding: utf-8 -*-
import socket
from os.path import getctime, getmtime
from datetime import datetime
from dateutil.tz import tzoffset

from simpledate import to_dt, from_ts
from validateutil import isurl

# replace the original socket.getaddrinfo by our version
origGetAddrInfo = socket.getaddrinfo
def getAddrInfoWrapper(host, port, family=0, socktype=0, proto=0, flags=0):
    return origGetAddrInfo(host, port, socket.AF_INET, socktype, proto, flags)
socket.getaddrinfo = getAddrInfoWrapper

from urllib.request import urlopen
from validateutil import isurl, ispickle
from os.path import splitext, normpath
import re

try:
    import magic
except ImportError:
    magic = type("magic", (object,), dict(from_buffer=lambda dum,*m,**y:[]))()
from mimetypes import guess_all_extensions, guess_type

__all__ = ("is_readable", "getorgdate","getfileobj", "getname", "getext", "getmime", "getext_guess_all",
           "is_appendonly_mode", "is_readable_mode", "is_readonly_mode", "is_readwrite_mode",
           "is_writable_mode", "is_writeonly_mode", "rewind_if_possible", "close_if_possible")


def is_readable(data, rule=("read", "close")):
    """
    data : url or path
    rule : object attribute list for "opened" defining
    """
    if len(set(rule) & set(dir(data))) > 1:
        return True
    elif hasattr(data, "fp"):
        return is_readable(data.fp)
    else:
        return False

def getopenner(data):
    if isurl(data) is True:
        return urlopen
    else:
        return open

def getfileobj(data, *args, **kwargs):
    if is_readable(data) is True:
        return data
    else:
        return getopenner(data)(data, *args, **kwargs) # note: getopenner return-> any open function

def getname(data, rule=("name", "url")):
    if is_readable(data) is False:
        if isurl(data) is True:
            return data
        else:
            return normpath(data)

    for name in (set(rule) & set(dir(data))):
        return normpath(getattr(data, name))

def filtered(name, exclusion):
    excl = [e.replace(".","") for e in exclusion]
    expr = "(?:\.{0}$)".format("$|\.".join(excl))
    return re.sub(expr, "", name)

def getext(data, exclusion=("bak","org")):
    name = getname(data)
    if isurl(name) is True:
        return None
    else:
        clearname = filtered(name, exclusion)
        return splitext(clearname)[1].replace(".","") or None

def getmode(fileobj):
    def extracter(fileobj, d = {"StringIO":"r+b", "StringI":"rb", "StringO":"w+b"}):
        if hasattr(fileobj, "mode"):
            return fileobj.mode
        elif "StringIO" in str(fileobj.__class__):
            return d[fileobj.__class__.__name__]

    def subfind(fileobj, subattr=("fp","io")):
        for sa in subattr:
            if hasattr(fileobj, sa):
                return extracter(getattr(fileobj, sa))

    if type(fileobj) == "_io.TextIOWrapper" and fileobj.name:
        return extracter(fileobj) or subfind(fileobj)

def is_readonly_mode(data):
    mode = getmode(data) or data
    if mode is not None:
        return "r" in mode and "+" not in mode
    else:
        raise TypeError("must be file object")

def is_writeonly_mode(data):
    mode = getmode(data) or data
    if mode is not None:
        return "w" in mode and "+" not in mode
    else:
        raise TypeError("must be file object")

def is_readwrite_mode(data):
    mode = getmode(data) or data
    if mode is not None:
        return "+" in mode
    else:
        raise TypeError("must be file object")

def is_appendonly_mode(data):
    mode = getmode(data) or data
    if mode is not None:
        return "a" in mode and "+" not in mode
    else:
        raise TypeError("must be file object")

def is_readable_mode(data):
    mode = getmode(data) or data
    if mode is not None:
        return "r" in mode or "+" in mode
    else:
        raise TypeError("must be file object")

def is_writable_mode(data):
    mode = getmode(data) or data
    if mode is not None:
        return "w" in mode or "+" in mode or "a" in mode
    else:
        raise TypeError("must be file object")

def rewind_if_possible(data):
    if is_readable(data) is True:
        if hasattr(data, "seek"):
            data.seek(0)
        elif hasattr(data.fp, "seek"):
            data.fp.seek(0)

def close_if_possible(data):
    if is_readable(data) is True:
        if hasattr(data, "close"):
            data.close()
        elif hasattr(data.fp, "close"):
            data.fp.close()

my_mimeType = {
               "pkl" : "application/x-python-pickle-data"
               }

def getmime(data, size_header=16):
    """Search MIME type from Any File, URL.
     **This Function Dependency in python-magic**
    data: filename or url or filepointer
      < Note When "data" is filepointer , Assumptions of zero position>
    size_header: (int) Byte to be read to determine
      < this Value related to the Guess accuracy goes up the more.
        Trade-off Latency.>
    """
    try:
        stream = getfileobj(data)
        head = stream.read(size_header)
    except IOError:
        return guess_type(getname(data), strict=False)[0] or my_mimeType.get(getext(data))
    else:
        ret = magic.from_buffer(head, mime=True)
        if ret is None:
            rewind_if_possible(stream)
            h = stream.read(2)
            stream.seek(-1,2) # EOF reverse read 1Byte
            e = stream.read(1)
            if ispickle(h + e) is True:
                ret = "application/x-python-pickle-data"
        elif data is not None and ret == "text/plain":
            try:
                ret = guess_type(getname(data), strict=False)[0]
            except TypeError:
                ret = None
        if isinstance(data, str) is True:
            close_if_possible(stream)
        rewind_if_possible(data)
        return ret

def getext_guess_all(data, strict=False):
    mime = getmime(data)
    return guess_all_extensions(mime, strict=strict) or list([getext(data)])




def testis_readable():
    assert is_readable(f) is True
    assert is_readable(r) is True
    assert is_readable(t) is False

def testopenner():
    assert getopenner(t) == open
    assert getopenner(r.url) == urlopen

def testgetfileobj():
    with getfileobj(t) as f:
        assert isinstance(f, file)
        f.close()
    res = getfileobj("http://www.google.com/index.html")
    assert hasattr(res,"url")
    res.close()

def testgetname():
    assert getname(f) == f.name
    assert getname(r) == normpath(r.url)
    assert getname(t) == t

def testfiltered():
    assert filtered("hoge.txt.bak",("bak","org")) == "hoge.txt"
    assert filtered("hoge.txt",("bak","org")) == "hoge.txt"
    assert filtered("org.bak.txt",("bak","org")) == "org.bak.txt"

def testgetext():
    assert getext(t) == "pkl"
    assert getext("hoge.txt.bak") == "txt"
    assert getext("hoge.txt") == "txt"
    assert getext("org.bak.txt") == "txt"
    assert getext("http://www.google.com") is None
    assert getext("hoge") is None
    assert getext(f) == "pkl"

def testgetmode():
    assert getmode(f) == "r"
    assert getmode(r) == "rb"
    assert getmode(t) == None
    assert getmode(s) == "r+b"

def testismodes():
    assert is_readonly_mode(f) == True
    assert is_readonly_mode(r) == True
    assert is_readonly_mode(s) == False
    assert is_writeonly_mode(f) == False
    assert is_writeonly_mode(r) == False
    assert is_writeonly_mode(s) == False
    assert is_readwrite_mode(f) == False
    assert is_readwrite_mode(r) == False
    assert is_readwrite_mode(s) == True
    assert is_appendonly_mode(f) == False
    assert is_appendonly_mode(r) == False
    assert is_appendonly_mode(s) == False

def testrewind_if_possible():
    f.read(3)
    assert f.tell() == 3
    rewind_if_possible(f)
    assert f.tell() == 0

def testclose_if_possible():
    assert f.closed is False
    close_if_possible(f)
    assert f.closed is True

def testgetmime():
    assert getmime("hoge.txt") == "text/plain"
    assert "pickle" in getmime("hoge.pkl")

def testgetext_guess_all(): #TODO
    pass

def testgetorgdate():
    assert getorgdate(testdir+"flickruploadtest.jpg") == to_dt("2000-08-04 18:22:57+09:00")
    assert isinstance(getorgdate(testdir+"test.m4v"),datetime)
    assert getorgdate("http://www.exif.org/samples/fujifilm-finepix40i.jpg") == to_dt("2000-08-04 18:22:57+09:00")

if __name__ == "__main__":
    from os.path import dirname
    from io import StringIO
    testdir = dirname(__file__) + "/../testdata/"
    testfile = normpath(testdir + "test.pkl")
    f = open(testfile)
    r = urlopen("http://www.google.com/index.html")
    t = testfile
    s = StringIO()

    testis_readable()
    testopenner()
    testgetfileobj()
    testgetname()
    testfiltered()
    testgetext()
    testgetmode()
    testismodes()
    testrewind_if_possible()
    testclose_if_possible()
    testgetmime()
    testgetext_guess_all()
