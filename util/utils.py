#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__  = 'm.yama'
__license__ = 'MIT'
__date__    = 'Wed Jul 31 17:35:47 2019'
__version__ = '0.0.1'

import os

BUF = 128 * 1024 ** 2

CHUNKSIZE = int(BUF / (64 * 40))

__all__ = [
    'command',
    'lsdir',
    'compressor',
    'decompressor',
    'geturi',
    'csvreader',
    'Counter',
    'timestamp2date',
    'which',
    'isnamedtuple',
    'values_at',
    'values_not',
    'vmfree',
    'compute_object_size',
    'islarge',
    'in_glob',
    're_zsplit',
    'path_norm',
    'sorter',
    'isposkey',
    'iterhead',
    'is1darray',
    'is2darray',
    'isdataframe',
    'sortedrows',
    'iterrows',
    'listlike',
    'kwtolist',
    'getdialect',
    'sniffer',
    'back_to_path',

]


import re
import sys
from datetime import datetime as dt
from itertools import chain, zip_longest
from glob import glob
from io import IOBase, StringIO, BytesIO
import fnmatch
from copy import deepcopy
import csv

from pathlib import Path
from collections import namedtuple, _count_elements

import gzip
from subprocess import getstatusoutput
try:
    import cloudpickle as pickle
except ImportError:
    import pickle

from util.core import flatten, binopen, opener, getencoding, isposix

def command(cmd):
    code, dat = getstatusoutput(cmd)
    if code == 0:
        return dat
    else:
        raise RuntimeError(dat)

def lsdir(path, recursive=True):
    func = "rglob" if recursive else "glob"
    for p in map(Path, glob(str(path))):
        yield p
        for r in p.__getattribute__(func)("*"):
            yield r

def compressor(obj, compresslevel=9):
    return gzip.compress(pickle.dumps(obj, protocol=-1), compresslevel=compresslevel)

def decompressor(obj):
    return pickle.loads(gzip.decompress(obj))

def geturi(s, file_prefix="file://"):
    if isinstance(s, Path):
        s = str(s)

    s = s.replace("\\", "/")

    if s[1] == ":":
        return file_prefix + "/" + s

    if s.startswith("//"):
        return file_prefix + s

    from urllib.parse import urlparse
    if len(urlparse(s).scheme) > 1:
        return s

    return file_prefix + os.path.abspath(s).replace("\\", "/")

def csvreader(itertable, encoding=None, delimiter=',',
         doublequote=True, escapechar=None, lineterminator='\r\n',
         quotechar='"', quoting=0,
         skipinitialspace=False, strict=False):

    userkw = {}
    decoder = lambda x, enc=encoding: x.decode(enc) if enc else x
    if delimiter != ',':
        if not delimiter:
            raise AttributeError("Invalid delimiter charactor. str or pattern")
        elif len(delimiter) == 1 or delimiter in ["\t", "\r", "\n", "\v", "a", "f"]:
            userkw["delimiter"] = delimiter
        else:
            userkw["delimiter"] = "\v"
            decoder = lambda x, enc=encoding, rep=re.compile(delimiter): rep.sub("\v", x.decode(enc) if enc else x)
    if doublequote      != True  : userkw["doublequote"] = doublequote
    if escapechar       != None  : userkw["escapechar"] = escapechar
    if lineterminator   != '\r\n': userkw["lineterminator"] = lineterminator
    if quotechar        != '"'   : userkw["quotechar"] = quotechar
    if quoting          != 0     : userkw["quoting"] = quoting
    if skipinitialspace != False : userkw["skipinitialspace"] = skipinitialspace
    if strict           != False : userkw["strict"] = strict

    return csv.reader(map(decoder, itertable), **userkw)

def Counter(*iterable):
    d = {}
    _count_elements(d, *iterable)
    return d

def timestamp2date(x, dfm = "%Y/%m/%d %H:%M"):
    return dt.fromtimestamp(x).strftime(dfm)

def which(executable):
    env = os.environ['PATH'].split(os.pathsep)
    exc = [".exe", ".bat", ".cmd", ".wsh", ".vbs"]
    for path in env:
        path = path.strip('"')

        fpath = os.path.join(path, executable)

        if os.path.isfile(fpath) and os.access(fpath, os.X_OK):
            return fpath

    if os.name == 'nt':
        if os.path.splitext(executable)[-1]:
            return None
        else:
            return next((which(executable + ext) for ext in exc), None)

def isnamedtuple(s):
    if not hasattr(s, "_fields"):
        return False
    if isinstance(s, type):
        raise ValueError("Not Instance named tuple")
    return True

def values_at(item:iter, k:list):
    if isinstance(item, dict):
        return {x: item[x] for x in set(item) & set(k)}
    elif isnamedtuple(item):
        if isposkey(k):
            return [item[x] for x in k]
        else:
            return [item.__getattribute__(x) for x in k]
    elif isinstance(item, (list, tuple)):
        if isposkey(k):
            return [item[x] for x in k]
        else:
            return [x for x in item if x in k]

def values_not(item:iter, k:list):
    if isinstance(item, dict):
        return {x: item[x] for x in set(item) - set(k)}
    elif isnamedtuple(item):
        if isposkey(k):
            not_idx = set(range(len(item))) - set(k)
            return [item[x] for x in not_idx]
        else:
            not_idx = set(item._fields) - set(k)
            return [item.__getattribute__(x) for x in not_idx]
    elif isinstance(item, (list, tuple)):
        if isposkey(k):
            not_idx = set(range(len(item))) - set(k)
            return [item[x] for x in not_idx]
        else:
            return [x for x in item if x not in k]

def vmfree():
    """ Virtual memory free size"""
    from psutil import virtual_memory
    return virtual_memory().available

def compute_object_size(o, handlers={}):
    from collections import deque

    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = sys.getsizeof(0)   # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = sys.getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)

def islarge(o):
    if compute_object_size(o) > BUF:
        return True
    if "dask" in str(type(o)):
        return True
    from psutil import Process
    rss = Process(os.getpid()).memory_info().rss
    if rss * 5 > vmfree():
        return True
    return False

def in_glob(srclst, wc):
    if not wc:
        return None

    if isinstance(wc , str):
        return fnmatch.filter(srclst, wc)

    return [s for s in srclst
                if next((s for w in wc
                    if fnmatch.fnmatch(s, w)), None)]

re_zsplit=re.compile('(.+(?:\\.gz|\\.gzip|\\.bz2|\\.xz|\\.lzma|\\.lzh|\\.lz|\\.tar|\\.tgz|\\.tz2|\\.zip|\\.rar|\\.7z|\\.Z|\\.cab|\\.dgc|\\.gca))[/\\\\]?(.*)', re.I)
def path_norm(f):
    rm = re_zsplit.search(f)
    if rm:
        a, b = rm.groups()
        return a, b or None
    else:
        return f, None

_xsorted = None
def sorter(o, *arg, **kw):
    if islarge(o):
        global _xsorted
        if _xsorted is None:
            try:
                from xsorted import xsorted # awesome
            except ModuleNotFoundError:
                sys.stderr.write("** No module warning **\nPlease Install command: pip3 install xsorted\n")
                xsorted = lambda *a, **k: iter(sorted(*a, **k))
            _xsorted = xsorted

        return _xsorted(o, *arg, **kw)
    else:
        return iter(sorted(o, *arg, **kw))

def isposkey(key):
    if not key:
        return False
        # raise ValueError("Can not check Empty Value. key is `{}`".format(key))
    return all(isinstance(k,int) for k in key)

def iterhead(iterator, n=1):
    if hasattr(iterator, "__next__") and n > 0 and isinstance(n, int):

        it = deepcopy(iterator)
        if n == 1:
            head = next(it)
        elif n > 1:
            head = [next(it) for i in range(n)]
        del it
        return head
    else:
        raise AttributeError

def is1darray(o):
    t = iterhead(o) if hasattr(o, "__next__") else o
    return "Series" in str(type(t)) or isinstance(t, (list, tuple)) and not isinstance(t[0], (list, tuple))

def is2darray(o):
    t = iterhead(o) if hasattr(o, "__next__") else o
    return isinstance(t, (list, tuple)) and isinstance(t[0], (list, tuple))

def isdataframe(o):
    return "DataFrame" in str(type(o))

def sortedrows(o, key=None, start=1, callback=list, header=False):
    """
    Return: sorted 2d generator -> tuple(rownumber, row)
    """
    rows = iterrows(o, start, callback=callback)
    if header:
        i, head = next(rows)

        if key:
            if callable(key):
                return chain([[i, head]], sorter(rows, key=lambda x: key(x[1])))
            pos = key if isposkey(key) else [head.index(k) for k in key]
            return chain([[i, head]], sorter(rows, key=lambda x: [x[1][k] for k in pos]))
        else:
            return chain([[i, head]], sorter(rows, key=lambda x: x[1]))
    else:
        if key:
            if callable(key):
                return sorter(rows, key=lambda x: key(x[1]))
            return sorter(rows, key=lambda x: [x[1][k] for k in key])
        else:
            return sorter(rows, key=lambda x: x[1])

def iterrows(o, start=1, callback=flatten):
    """
    Return: 2d generator -> tuple(rownumber, row)
    """

    if isdataframe(o):
        if isinstance(start, int):
            rows = (callback(x[1:]) for x in o.fillna("").itertuples())
            header = ([start, list(o.columns)],)
            return chain(header, ([i, callback(x)] for i,x in enumerate(rows, start+1)))
        elif start == "infer":
            rows = ([x[0], callback(x[1:])] for x in o.fillna("").itertuples(False))
            header = ([1, callback(o.columns)],)
            return chain(header, rows)
        elif hasattr(start, "__iter__"):
            rows = (callback(x[1:]) for x in o.fillna("").itertuples())
            header = ([1, callback(o.columns)],)
            return chain(header, zip_longest(start, rows))
        else:
            rows = (callback(x) for x in o.fillna("").itertuples(False, None))
            header = [callback(o.columns)]
            return chain(header, rows)
    if hasattr(o, "__next__"):
        head = [next(o)]
        o = chain(head, o)
    else:
        head = [o[0]]

    if is2darray(head):
        if isinstance(start, int):
            return iter([i, callback(x)] for i, x in enumerate(o, start))
            # return map(lambda i,*x: [i, callback(x)], enumerate(o, start))
        elif start == "infer":
            return ([x[0], callback(x[1:])] for x in o)
        elif hasattr(start, "__iter__"):
            return zip_longest(start, map(callback,o))
        else:
            return iter(callback(x) for x in o)
    else:
        if isinstance(start, int):
            return enumerate(o, start)
        elif start == "infer":
            return ([x[0], x[1:]] for x in o)
        elif hasattr(start, "__iter__"):
            return zip_longest(start, o)
        else:
            return iter(o)


def listlike(iterator, callback=None):
    class Slice(object):
        def __init__(self, iterator, callback=None):
            self._iter = iterator
            self.callback = callback or (lambda x: x)
            self.__root = None
            self._length = None
            self._cache = []

        @property
        def _root(self):
            if self.__root is None:
                self.__root = deepcopy(self._iter)
            return self.__root

        def __getitem__(self, k):
            if isinstance(k, slice):
                if (k.start or 0) < 0 or (k.stop or -1) < 0:
                    self._get_value(-1)
                    return self._cache[k.start: k.stop: k.step or 1]
                try:
                    return [self._get_value(i) for i in range(k.start or 0, k.stop, k.step or 1)]
                except StopIteration:
                    return self._cache[k.start: k.stop: k.step or 1]

            return self._get_value(k)

        def _get_value(self, k):
            cache_len = len(self._cache)

            if k >= 0 and k < cache_len:
                return self.callback(self._cache[k])

            ret = None

            if k < 0:
                self._cache.extend(list(self._root))
                ret = self._cache[k]
            else:
                for _ in range(k - cache_len + 1):
                    ret = next(self._root)
                    self._cache.append(ret)
            return self.callback(ret)

        def __next__(self):
            return self.callback(next(self._iter))

        def __len__(self):
            if self._length is None:
                root_copy = deepcopy(self._root)
                self._length = len(self._cache) + sum(1 for _ in root_copy)
                del root_copy
            return self._length

        def cacheclear(self):
            self._cache = []
            self.__root = []

        def __del__(self):
            self.cacheclear()

    if isinstance(iterator, Slice):
        return iterator
    elif isinstance(iterator, (tuple, list)):
        if callback is None:
            return iterator
        else:
            return type(iterator)(callback(x) for x in iterator)
    elif hasattr(iterator, "__next__"):
        return Slice(iterator, callback)
    else:
        raise ValueError("Unknown type is {}".format(iterator))



def kwtolist(key, start=1):
    if not key:
        return
    if isinstance(key, (list, tuple)):
        return key
    elif re.search("[^0-9,\-]", key):
        return key.split(",")
    ret = []
    for x in key.split(","):
        if "-" in x:
            s,e = x.split("-")
            ret.extend([i + (start * -1) for i in range(int(s),int(e)+1)])
        else:
            ret.append(int(x) + (start * -1))

    return ret

PROP_HEADER = ["fullpath", "parent", "basename", "extention",
          "owner", "group", "permision",
          "cdate", "mdate", "filesize"] + ["DIR"+str(i) for i in range(1,11)]

dirs = [None] * 10
fifo = namedtuple("FileProp", tuple(PROP_HEADER))
kifo = namedtuple("FileSniff",
                  ('sep', 'encoding', 'lineterminator', 'quoting', 'doublequote','delimiter','quotechar'))
fkifo = namedtuple("FileExProp", fifo._fields+kifo._fields)
difo = namedtuple("DirSniff", ('count', 'size'))
fdifo = namedtuple("DirExProp", fifo._fields+difo._fields)

def getdialect(dat:bytes):
    enc = getencoding(dat)
    if enc is None:
        raise ValueError("Cannot get dialect of Binary File.")
    txt = dat.decode(enc)
    return csv.Sniffer().sniff(txt)

def sniffer(dat:bytes):
    enc = getencoding(dat)
    if enc is None:
        raise ValueError("Cannot get dialect of Binary File.")
    d = csv.Sniffer().sniff(dat.decode(enc))

    if d:
        return kifo(d.delimiter, enc, d.lineterminator, d.quoting, d.doublequote, d.delimiter, d.quotechar)
    else:
        return kifo(None, "binary", None, None, None, None, None)

unuri = re.compile("(?:file:/*)(?:([A-Za-z]):?)?(?=/)(.*)")
def back_to_path(uri:str):
    try:
        ret = unuri.findall(uri.replace("\\", "/"))[0]
        return ":".join(ret) if ret[0] else ret[1]
    except IndexError:
        return uri






if __name__ == "__main__":

    def test():
        from datetime import datetime as dt
        from pathlib import Path
        from util.core import tdir, TMPDIR

        def test_lsdir():
            def _testargs(func, pathstr, *args):
                #TODO assert
                ret = [
                    func(pathstr, *args),
                    func(Path(pathstr), *args),
                    func(Path(pathstr), *args),
                ]
                try:
                    with open(pathstr) as f:
                        ret.append(func(f, *args))
                except Exception as e:
                    ret.append(e)
                return ret
            _testargs(lsdir, tdir)
            _testargs(lsdir, tdir + "diff*")
            _testargs(lsdir, tdir+"test.csv")
            _testargs(lsdir, tdir+"*est.csv")
            _testargs(lsdir, tdir+"ddfghjdtui")
            _testargs(lsdir, tdir, False)
            _testargs(lsdir, tdir + "diff*", False)
            _testargs(lsdir, tdir+"test.csv", False)
            _testargs(lsdir, tdir+"*est.csv", False)
            _testargs(lsdir, tdir+"ddfghjdtui", False)

        def test_getencoding():
            with open(tdir+"diff1.csv", "rb") as f:
                assert(getencoding(f.read()) == "cp932")
            with open(tdir+"diff2.csv", "rb") as f:
                assert(getencoding(f.read()) == "cp932")
            with open(tdir+"test_utf8.csv", "rb") as f:
                assert(getencoding(f.read()) == "utf-8")
            with open(tdir+"sample.sqlite3", "rb") as f:
                assert(getencoding(f.read()) is None)

        def test_geturi():
            assert(geturi(tdir).rstrip("/") == "file://" + (isposix is False and "/" or "") + tdir.replace("\\", "/").rstrip("/"))
            Path(geturi(tdir+"test.zip"))
            assert(geturi(r"Z:\temp\hoge.txt") == "file:///Z:/temp/hoge.txt")

        def test_getdialect():
            with open(tdir+"diff1.csv", "rb") as f:
                assert(getdialect(f.read()).delimiter == ",")


        def test_sniffer():
            from csv import QUOTE_MINIMAL
            with open(tdir+"diff1.csv", "rb") as f:
                assert(sniffer(f.read()) == kifo(sep=',', encoding='cp932', lineterminator='\r\n', quoting=QUOTE_MINIMAL, doublequote=False, delimiter=',', quotechar='"'))

        def test_back_to_path():
            uris = ["file:/Y:/usr/share/testdata/test.zip",
                     "file://Y:/usr/share/testdata/test.zip",
                     "file:///Y:/usr/share/testdata/test.zip",
                     "file:///Y/usr/share/testdata/test.zip",
                     "file:/usr/share/testdata/test.zip",
                     "file://usr/share/testdata/test.zip",
                     "file:///usr/share/testdata/test.zip",
                     "http://www/google.com",
                     r"file:\Y:\usr\share\testdata\test.zip"]

            assert(list(map(back_to_path, uris)) == [
                    "Y:/usr/share/testdata/test.zip",
                    "Y:/usr/share/testdata/test.zip",
                    "Y:/usr/share/testdata/test.zip",
                    "Y:/usr/share/testdata/test.zip",
                    "/usr/share/testdata/test.zip",
                    "/usr/share/testdata/test.zip",
                    "/usr/share/testdata/test.zip",
                    "http://www/google.com",
                    "Y:/usr/share/testdata/test.zip"])

        def test_binopen():
            pass

        def test_opener():
            import traceback

            def tests(func):
                f = tdir + "diff2.csv"

                with open(f, "r") as ff:
                    assert(isinstance(func(ff), IOBase))

                with open(f, "rb") as ff:
                    assert(isinstance(func(ff), IOBase))

                try:
                    w = os.path.join(TMPDIR, "writetest")

                    with open(w, "w") as ww:
                        assert(isinstance(func(ww), IOBase))

                    with open(w, "wb") as ww:
                        assert(isinstance(func(ww), IOBase))
                except:
                    traceback.print_exc()
                    raise
                finally:
                    os.remove(w)

                with StringIO("aa") as s:
                    assert(isinstance(func(s), IOBase))

                with BytesIO(b"aa") as b:
                    assert(isinstance(func(b), IOBase))

                try:
                    assert(isinstance(func("/hoge/foo"), IOBase))
                except FileNotFoundError:
                    pass

            tests(opener)
            tests(binopen)

        def test_flatten():
            assert(flatten([0,1,2]) == [0,1,2])
            assert(flatten([[0,1,2]]) == [0,1,2])
            assert(flatten([[0,1],2]) == [0,1,2])
            assert(flatten([[0,1],[[2]]]) == [0,1,2])

        def test_timestamp2date():
            assert(timestamp2date(140400) == "1970/01/03 00:00")
            assert(timestamp2date(2**31 - 1) == "2038/01/19 12:14")
            try:
                timestamp2date("1")
            except TypeError:
                pass
            except:
                raise AssertionError
            else:
                raise AssertionError

        def test_which():
            assert("python" in which("python"))
            assert(which("dfhjksahjfklsahnfkjl") is None)

        def test_isnamedtuple():
            n = namedtuple("hoo", ["bar", "hoge"])
            try:
                isnamedtuple(n)
            except ValueError:
                pass
            except:
                raise AssertionError
            else:
                raise AssertionError
            r = n(1,2)
            assert(isnamedtuple(r) is True)
            assert(isnamedtuple([1]) is False)

        def test_values_at():
            assert(values_at(dict(a=1,b=2,c=3), ["a", "c"]) == {'a': 1, 'c': 3})
            assert(values_at(["a", "b", "c"], ["a", "c"]) == ["a", "c"])
            assert(values_at(["a", "b", "c"], [0,2]) == ["a", "c"])
            t = namedtuple("test", list("abc"))
            assert(values_at(t(1,2,3), [0, 2]) == [1, 3])
            assert(values_at(t(1,2,3), ["a", "c"]) == [1, 3])


        def test_values_not():
            assert(values_not(dict(a=1,b=2,c=3), ["a", "c"]) == {'b': 2})
            assert(values_not(["a", "b", "c"], ["a", "c"]) == ["b"])
            assert(values_not(["a", "b", "c"], [0,2]) == ["b"])
            t = namedtuple("test", list("abc"))
            assert(values_not(t(1,2,3), [0, 2]) == [2])
            assert(values_not(t(1,2,3), ["a", "c"]) == [2])

        def test_vmfree():
            assert(vmfree() > 0)

#        def test_compute_object_size():
#            assert(compute_object_size(_handler_zopen.archived_magic_numbers) > 0)

#        def test_islarge():
#            assert(islarge([]) is False)
#            global BUF
#            BUF = 10
#            assert(islarge(_handler_zopen.archived_magic_numbers) is True)
#            BUF = 128 * 1024 ** 2


        def test_in_glob():
            assert(in_glob(["abc.txt", "hoge.csv"], "*.txt") == ["abc.txt"])
            assert(in_glob(["abc.txt", "hoge.csv"], ["*.txt", "abc*"]) == ["abc.txt"])
            assert(in_glob(["abc.txt", "hoge.csv"], ["*.*", "*.txt"]) == ["abc.txt", "hoge.csv"])
            assert(in_glob(["abc.txt", "hoge.csv"], ["*.*", "*"]) == ["abc.txt", "hoge.csv"])
            assert(in_glob(["abc.txt", "hoge.csv"], ["foo", "bar"]) == [])
            assert(in_glob(["abc.txt", "hoge.csv"], []) == None)

        def test_path_norm():
            assert(path_norm("/hoge/test/test.zip") == ('/hoge/test/test.zip', None))
            assert(path_norm("/hoge/test/test.zip/foo.txt") == ('/hoge/test/test.zip', "foo.txt"))
            assert(path_norm(r"\\hoge\test\test.zip\foo.txt") == (r'\\hoge\test\test.zip', "foo.txt"))
            assert(path_norm("") == ("", None))
            try:
                path_norm(None)
            except TypeError:
                pass
            except:
                raise AssertionError
            else:
                raise AssertionError
        def test_sorter():
            pass

        def test_isposkey():
            assert(isposkey([0,1]) is True)
            assert(isposkey([0,"1"]) is False)
            assert(isposkey([]) is False)
            assert(isposkey(None) is False)

        def test_iterhead():
            a = iter(list("abc"))
            h = iterhead(a)
            assert((h, list(a)) == ("a", ['a', 'b', 'c']))

        def test_is1darray():
            assert(is1darray([1,2]))
            assert(is1darray([[1],[2]]) is False)

        def test_is2darray():
            assert(is2darray([1,2]) is False)
            assert(is2darray([[1],[2]]))
            assert(is2darray([tuple("abc"), tuple("def")]))
            assert(is2darray((tuple("abc"), tuple("def"))))

        def test_isdataframe():
            import pandas as pd
            assert(isdataframe(pd.DataFrame()))
            assert(isdataframe([]) is False)
            assert(isdataframe({1:1}) is False)
            assert(isdataframe(pd.Series()) is False)

        def test_sortedrows():
            assert(list(sortedrows(iter([[3],[2],[5],[1]]))) == [[4, [1]], [2, [2]], [1, [3]], [3, [5]]])
            assert(list(sortedrows(iter([[3],[2],[5],[1]]),start=0)) == [[3, [1]], [1, [2]], [0, [3]], [2, [5]]])
            assert(list(sortedrows(iter([[3],[2],[5],[1]]))) == [[4, [1]], [2, [2]], [1, [3]], [3, [5]]])
            assert(list(sortedrows(iter([[1,3],[2,2],[3,5],[4,1]]), lambda x: x[1])) == [[4, [4, 1]], [2, [2, 2]], [1, [1, 3]], [3, [3, 5]]])
            assert(list(sortedrows(iter([[3],[2],[5],[1]]), start=0)) == [[3, [1]], [1, [2]], [0, [3]], [2, [5]]])
            assert(list(sortedrows([[3,3],[2,2],[1,1]], header=False)) == [[3, [1, 1]], [2, [2, 2]], [1, [3, 3]]])
            assert(list(sortedrows([[3,3],[2,2],[1,1]], header=True)) == [[1, [3, 3]], [3, [1, 1]], [2, [2, 2]]])
            assert(list(sortedrows([[3,"b"],[2,"a"],[1,"c"]], header=False, key=lambda x: x[1])) == [[2, [2, 'a']], [1, [3, 'b']], [3, [1, 'c']]])
            assert(list(sortedrows([[3,"b"],[2,"c"],[1,"a"]], header=True, key=lambda x: x[1])) == [[1, [3, 'b']], [3, [1, 'a']], [2, [2, 'c']]])



        def test_iterrows():
            from util.io import readrow
            a = iter(list("abc"))
            h = iterrows(a,None)
            assert(list(h) == ['a', 'b', 'c'])
            assert(list(iterrows([tuple("abc"), tuple("def")])) == [[1, ['a', 'b', 'c']], [2, ['d', 'e', 'f']]])
            assert(list(iterrows([tuple("abc"), tuple("def")])) == [[1, ['a', 'b', 'c']], [2, ['d', 'e', 'f']]])
            assert(list(iterrows([[3,*list("abc")],[9,*list("abc")]],start="infer")) == [[3, ["a", "b", "c"]], [9, ["a", "b", "c"]]])

            f = tdir + "diff1.csv"
            idxcheck = [0,9,2]
            a = list(zip(idxcheck, list(readrow(f))[:3]))
            h = iterrows(a,"infer")

            for i, hh in enumerate(h):
                assert(len(hh) == 2)
                assert(hh[0] == idxcheck[i])


        def test_listlike():
            a = iter([1,2,3])
            try:
                len(a)
            except TypeError:
                pass
            b = listlike(a)
            assert(len(b) == 3)
            assert(b[0] == 1)
            assert(list(b) == [1,2,3])

            a = iter(list("abcd"))
            r = listlike(a)
            assert(r[0] == "a")
            assert(r[0] == "a")
            assert(len(r) == 4)
            assert(r[1:] == list("bcd"))
            assert(r[:-1] == list("abc"))
            assert(r[-1:] == ["d"])
            assert(r[:3] == list("abc"))
            assert(list(a) == list("abcd"))
            assert(list(a) == [])

        def test_kwtolist():
            assert(kwtolist("a,b,c") == list("abc"))
            assert(kwtolist("1,2,3", 0) == [1,2,3])
            assert(kwtolist("1-3", 0) == [1,2,3])
            assert(kwtolist("1,2,3") == [0,1,2])
            assert(kwtolist("1-3,5") == [0,1,2,4])
            assert(kwtolist("1-3,5",0) == [1,2,3,5])

        def __test_wrap(wrapobj):
            assert(len(list(wrapobj.lsdir()))>0)
            assert(len(list(wrapobj.tree_file()))>0)
            wrapobj.tree_dir()
            for infile in wrapobj:
                anser = b'n,aa\r\n1,1\r\n2,\x82\xa0\r\n'
                assert(infile.read() == anser)
                infile.seek(0)
                assert(infile.read_bytes() == anser)
                infile.seek(0)
                assert(infile.read_text() == anser.decode("cp932"))
                assert(isinstance(infile.getinfo(),fifo))
                assert(isinstance(infile.getinfo(True), fkifo))
                infile.seek(0)
                assert(infile.encoding)
                assert(getencoding(infile.read()))
                infile.seek(0)
                infile.extract(TMPDIR)
                with open(Path(TMPDIR).joinpath("test.csv"),"rb") as f:
                    assert(f.read() == anser)


        t0 = dt.now()
        for x, func in list(locals().items()):
            if x.startswith("test_") and callable(func):
                t1 = dt.now()
                func()
                t2 = dt.now()
                print("{} : time {}".format(x, t2-t1))
        t3 = dt.now()
        print("{} : time {}".format(x, t3-t0))
    test()

