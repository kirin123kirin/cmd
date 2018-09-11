# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""


import os

isposix = os.name == "posix"

BUF = 128 * 1024 ** 2

CHUNKSIZE = int(BUF / (64 * 40))

__all__ = [
    "TMPDIR",
    "lsdir",
    "getencoding",
    "getsize",
    "geturi",
    "binopen",
    "opener",
    "flatten",
    "timestamp2date",
    "which",
    "isnamedtuple",
    "values_at",
    "values_not",
    "vmfree",
    "compute_object_size",
    "logger",
    "islarge",
    "in_glob",
    "path_norm",
    "sorter",
    "isposkey",
    "iterhead",
    "is1darray",
    "is2darray",
    "isdataframe",
    "sortedrows",
    "iterrows",
    "listlike",
    "kwtolist",
    "fifo",
    "kifo",
    "fkifo",
    "difo",
    "fdifo",
    "getdialect",
    "sniffer",
    "Path",
    "ZipArchiveWraper",
    "TarArchiveWraper",
    "LhaArchiveWraper",
    "RarArchiveWraper",
    "ZLibArchiveWraper",
    "ZipExtFile",
    "ZipFile",
    "TarFile",
    "RarFile",
    "LhaInfo",
    "LhaFile",
    "GzipFile",
    "LZMAFile",
    "BZ2File",
    "is_compress",
    "zopen",
    "zopen_recursive",
]

import re
import sys
import codecs
import io
import csv
from datetime import datetime as dt
from psutil import virtual_memory, Process
from urllib.parse import urlparse
from itertools import chain, zip_longest
import fnmatch
from mimetypes import guess_type, guess_extension, guess_all_extensions
from collections import deque
from io import IOBase, StringIO, BytesIO
from itertools import tee
from copy import copy
from six import string_types

import pathlib
from glob import glob
from collections import namedtuple

import gzip
import tarfile
from tarfile import ExFileObject
import bz2
import zipfile
import lzma

import traceback


# 3rd party modules
import nkf

try:
    from xsorted import xsorted   # awesome
except ModuleNotFoundError:
    sys.stderr.write("** No module warning **\nPlease Install command: pip3 install xsorted\n")
    xsorted = lambda *a, **k: iter(sorted(*a, **k))


class UnsupportCompressError(RuntimeError):
    pass

class NonCompressedError(ValueError):
    pass

def lsdir(path, recursive=True):
    subfunc = "rglob" if recursive else "glob"
    for f in glob(str(path)):
        p = Path(f)
        yield p
        for r in p.__getattribute__(subfunc)("*"):
            yield r

def getencoding(dat:bytes):
    enc = nkf.guess(dat).lower()
    if enc and enc == "shift_jis":
        return "cp932"
    elif enc == "binary":
        return None
    else:
        return enc

def getsize(fp):
    p = fp.tell()
    fp.seek(0, os.SEEK_END)
    size = fp.tell()
    fp.seek(p)
    return size

def geturi(s):
    if isinstance(s, Path):
        s = str(s.as_posix())

    if len(urlparse(s).scheme) > 1:
        return s
    elif s.startswith(r"\\") or s.startswith("//"):
        return "file://" + s.replace("\\", "/")
    else:
        p = pathlib.Path(s)

        if not p.is_absolute():
            p = p.resolve()

        return p.as_uri().replace("%5C", "/")

def binopen(f, mode="rb", *args, **kw):
    if isinstance(f, (ExFileObject, _baseArchive, BytesIO)):
        return f
    elif isinstance(f, IOBase):
        if isinstance(f, StringIO):
            e = f.encoding
            if e:
                return BytesIO(f.getvalue().encode(e))
            else:
                return BytesIO(f.getvalue().encode())
        m = f.mode
        if isinstance(m, int):
            return f

        name = f.name
        p = f.tell()

        if "b" in m:
            return f
        else:
            f.seek(0)
            r = open(name, mode=m + "b", *args, **kw)
            r.seek(p)
            return r
    elif isinstance(f, str):
        return open(pathlib.Path(f), mode, *args, **kw)
    elif isinstance(f, pathlib.Path):
        return open(f, mode, *args, **kw)
    else:
        print()
        raise ValueError("Unknown Object. filename or filepointer buffer")

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
    elif isinstance(f, (Path, str)):
        return codecs.open(f, mode=mode.replace("b", ""), *args, **kw)
    else:
        raise ValueError("Unknown Object. filename or filepointer buffer")

def flatten(iterable, base_type=None, levels=None):
    """Flatten an iterable with multiple levels of nesting (e.g., a list of
    lists of tuples) into non-iterable types.

        >>> iterable = [(1, 2), ([3, 4], [[5], [6]])]
        >>> list(flatten(iterable))
        [1, 2, 3, 4, 5, 6]

    String types are not considered iterable and will not be flattend.
    To avoid collapsing other types, specify *base_type*:

        >>> iterable = ['ab', ('cd', 'ef'), ['gh', 'ij']]
        >>> list(flatten(iterable, base_type=tuple))
        ['ab', ('cd', 'ef'), 'gh', 'ij']

    Specify *levels* to stop flattening after a certain level:

    >>> iterable = [('a', ['b']), ('c', ['d'])]
    >>> list(flatten(iterable))  # Fully flattened
    ['a', 'b', 'c', 'd']
    >>> list(flatten(iterable, levels=1))  # Only one level flattened
    ['a', ['b'], 'c', ['d']]

    """
    def walk(node, level):
        if (
            ((levels is not None) and (level > levels)) or
            isinstance(node, string_types) or
            ((base_type is not None) and isinstance(node, base_type))
        ):
            yield node
            return

        try:
            tree = iter(node)
        except TypeError:
            yield node
            return
        else:
            for child in tree:
                for x in walk(child, level + 1):
                    yield x

    return list(walk(iterable, 0))


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
            for ext in exc:
                ret = which(executable + ext)
                if ret:
                    return ret

    return None

def isnamedtuple(s):
    if not hasattr(s, "_fields"):
        return False
    if isinstance(s, type):
        raise ValueError("Not Instance named tuple")
    return True

def values_at(item:iter, k:list):
    if isinstance(item, dict):
        return {x: item[x] for x in set(item) & set(k)}
    elif isinstance(item, list):
        if isposkey(k):
            return [item[x] for x in k]
        else:
            return [x for x in item if x in k]
    elif isnamedtuple(item):
        if isposkey(k):
            return [item[x] for x in k]
        else:
            return [item.__getattribute__(x) for x in k]

def values_not(item:iter, k:list):
    if isinstance(item, dict):
        return {x: item[x] for x in set(item) - set(k)}
    elif isinstance(item, list):
        if isposkey(k):
            not_idx = set(range(len(item))) - set(k)
            return [item[x] for x in not_idx]
        else:
            return [x for x in item if x not in k]
    elif isnamedtuple(item):
        if isposkey(k):
            not_idx = set(range(len(item))) - set(k)
            return [item[x] for x in not_idx]
        else:
            not_idx = set(item._fields) - set(k)
            return [item.__getattribute__(x) for x in not_idx]

def vmfree():
    """ Virtual memory free size"""
    return virtual_memory().available

def compute_object_size(o, handlers={}):
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

LOGGERLEVEL_DEBUG = 0
LOGGERLEVEL_INFO = 1
LOGGERLEVEL_WARN = 2
LOGGERLEVEL_ERROR = 3
LOGGERLEVEL_CRITICAL = 4

class logger(object):
    def __init__(self, filepath_or_buffer=None, loglevel=LOGGERLEVEL_INFO, autoclose=True):
        self.loglevel = loglevel
        self.autoclose = autoclose
        if filepath_or_buffer is None:
            self.con = sys.stdout
        elif filepath_or_buffer in [sys.stdout, sys.stderr]:
            self.con = filepath_or_buffer
        else:
            self.con = opener(filepath_or_buffer, "w")
    def close(self):
        if hasattr(self.con, "close") and self.autoclose:
            self.con.close()
    def write(self, s, loglevel=LOGGERLEVEL_INFO):
        if self.loglevel <= loglevel:
            self.con.write(s)
    def writelines(self, s, loglevel=LOGGERLEVEL_INFO):
        if self.loglevel <= loglevel:
            self.con.writelines(s)
    def __call__(self, s, loglevel=LOGGERLEVEL_INFO):
        self.write(s, loglevel)
    def __getattr__(self,name):
        return self.con.__getattribute__(name)
    def __enter__(self):
        return self
    def __del__(self):
        self.close()
    def __exit__(self, exc_type, exc_value, tb):
        if exc_type:
            sys.stderr.write("{}\n{}\n{}".format(exc_type, exc_value, tb))
        self.close()

def islarge(o):
    if compute_object_size(o) > BUF:
        return True
    if "dask" in str(type(o)):
        return True
    rss = Process(os.getpid()).memory_info().rss
    if rss * 5 > vmfree():
        return True
    return False


def in_glob(srclst, wc):
    if not wc:
        return None
    if isinstance(wc , str):
        return fnmatch.filter(srclst, wc)

    ret = []
    sl = srclst.copy()
    while sl:
        src = sl.pop(0)
        for w in wc:
            if fnmatch.fnmatch(src, w):
                ret.append(src)
                break
    return ret

re_zsplit=re.compile('(.+(?:\\.gz|\\.gzip|\\.bz2|\\.xz|\\.lzma|\\.lzh|\\.lz|\\.tar|\\.tgz|\\.tz2|\\.zip|\\.rar|\\.7z|\\.Z|\\.cab|\\.dgc|\\.gca))[/\\\\]?(.*)', re.I)
def path_norm(f):
    rm = re_zsplit.search(f)
    if rm:
        a, b = rm.groups()
        return a, b or None
    else:
        return f, None


def sorter(o, *arg, **kw):
    if islarge(o):
        return xsorted(o, *arg, **kw)
    else:
        return iter(sorted(o, *arg, **kw))

def isposkey(key):
    if not key:
        return False
        # raise ValueError("Can not check Empty Value. key is `{}`".format(key))
    return all(isinstance(k,int) for k in key)

def iterhead(iterator, n=1):
    if hasattr(iterator, "__next__") and n > 0 and isinstance(n, int):
        it = copy(iterator)
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
    return "Series" in str(type(t)) or isinstance(t, list) and not isinstance(t[0], list)

def is2darray(o):
    t = iterhead(o) if hasattr(o, "__next__") else o
    return isinstance(t, list) and isinstance(t[0], list)

def isdataframe(o):
    return "DataFrame" in str(type(o))

def sortedrows(o, key=None, start=1):
    """
    Return: sorted 2d generator -> tuple(rownumber, row)
    """
    if is1darray(o):
        return sorter(iterrows(o, start), key=key or (lambda x: x[1]))
    rows      = iterrows(o, start)
    i, header = next(rows)
    if key:
        pos = key if isposkey(key) else [header.index(k) for k in key]
        return chain([(i, header)], sorter(rows, key=lambda x: [x[1][k] for k in pos]))
    else:
        return chain([(i, header)], sorter(rows))

def iterrows(o, start=1):
    """
    Return: 2d generator -> tuple(rownumber, row)
    """

    if isdataframe(o):
        if isinstance(start, int):
            rows = (list(x[1:]) for x in o.fillna("").itertuples())
            header = ([start, list(o.columns)],)
            return chain(header, enumerate(rows, start+1))
        elif start == "infer":
            rows = ([x[0], list(x[1:])] for x in o.fillna("").itertuples(False))
            header = ([1, list(o.columns)],)
            return chain(header, rows)
        elif hasattr(start, "__iter__"):
            rows = (list(x[1:]) for x in o.fillna("").itertuples())
            header = ([1, list(o.columns)],)
            return chain(header, zip_longest(start, rows))
        else:
            rows = (list(x) for x in o.fillna("").itertuples(False, None))
            header = [list(o.columns)]
            return chain(header, rows)
    else:
        if isinstance(start, int):
            return enumerate(o, start)
        elif start == "infer":
            header = [[1, o[0][1:]]]
            return chain(header, ([x[0], x[1:]] for x in o[1:]))
        elif hasattr(start, "__iter__"):
            return zip_longest(start, o)
        else:
            return iter(o)


def listlike(iterator):
    class Slice(object):
        def __init__(self, iter):
            self._iter, self._root = tee(iter)
            self._length = None
            self._cache = []
        def __getitem__(self, k):
            if isinstance(k, slice):
                return [self._get_value(i) for i in range(k.start, k.stop, k.step or 1)]
            return self._get_value(k)
        def _get_value(self, k):
            cache_len = len(self._cache)

            if k < cache_len:
                return self._cache[k]

            self._root, root_copy = tee(self._root)
            ret = None

            for _ in range(k - cache_len + 1):
                ret = next(root_copy)
                self._cache.append(ret)

            self._root = root_copy
            return ret
        def __next__(self):
            return next(self._iter)
        def __len__(self):
            if self._length is None:
                self._iter, root_copy = tee(self._iter)
                self._length = sum(1 for _ in root_copy)
                del root_copy
            return self._length
        def cacheclear(self):
            self._cache = []

    return Slice(iterator)

def kwtolist(key, start=1):
    if not key:
        return
    if isinstance(key, list):
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
    enc = nkf.guess(dat).lower()
    if enc == "shift_jis":
        enc = "cp932"
    if enc == "binary":
        return None
    else:
        return csv.Sniffer().sniff(dat.decode(enc))

def sniffer(dat:bytes):
    d = getdialect(dat)
    if d:
        return kifo(d.delimiter, getencoding(dat), d.lineterminator, d.quoting, d.doublequote, d.delimiter, d.quotechar)
    else:
        return kifo(None, "binary", None, None, None, None, None)

unuri = re.compile("(?:file:/*)(?:([A-Za-z]):?)?(?=/)(.*)")
def back_to_path(uri:str):
    try:
        ret = unuri.findall(uri.replace("\\", "/"))[0]
        return ":".join(ret) if ret[0] else ret[1]
    except IndexError:
        return uri

class PathList(list):
    raiseop = ["mkdir", "replace", "rmdir", "write_text","write_bytes", "unlink"]
    pathop = ["encoding", "ext", "dialect", "lineterminator",
                       "quoting", "doublequote", "delimiter", "quotechar"]

    def __init__(self, initlist=None):
        if isinstance(initlist, list):
            initlist = flatten(initlist)
        elif isinstance(initlist, Path):
            initlist = [initlist]

        super().__init__(initlist)

    def __dir__(self):
        return self.pathop + super().__dir__()

    def samefile(self, other_path):
        return [x.samefile(other_path) for x in self if x.is_file()]

    def iterdir(self):
        for x in self:
            if x.is_dir():
                for y in x.iterdir():
                    yield y

    def glob(self, pattern):
        return flatten(x.glob(pattern) for x in self if x.is_dir())

    def rglob(self, pattern):
        return flatten(x.rglob(pattern) for x in self if x.is_dir())

    def absolute(self):
        return [x.absolute() for x in self]

    def resolve(self, strict=False):
        return [x.resolve(strict) for x in self]

    def stat(self):
        return [x.stat() for x in self]

    def owner(self):
        return [x.owner() for x in self]

    def group(self):
        return [x.group() for x in self]

    def open(self, mode='r', buffering=-1, encoding=None,
                      errors=None, newline=None):
        return [x.open(mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline) for x in self if x.is_file()]

    def read_bytes(self, n=-1):
        return [x.read_bytes(n) for x in self if x.is_file()]

    def read_text(self, n=-1, encoding=None, errors=None):
        return [x.read_text(n, encoding=encoding, errors=errors) for x in self if x.is_file()]
    read = read_text

    def lstat(self):
        return [x.lstat() for x in self]

    def exists(self):
        return [x.exists() for x in self]

    def is_dir(self):
        return [x.is_dir() for x in self]

    def is_file(self):
        return [x.is_file() for x in self]

    def is_symlink(self):
        return [x.is_symlink() for x in self]

    def is_block_device(self):
        return [x.is_block_device() for x in self]

    def is_char_device(self):
        return [x.is_char_device() for x in self]

    def is_fifo(self):
        return [x.is_fifo() for x in self]

    def is_socket(self):
        return [x.is_socket() for x in self]

    def expanduser(self):
        return [x.expanduser() for x in self]

    def unlink(self):
        return [x.unlink() for x in self]

    def delete(self):
        return [x.delete() for x in self]
    
    def lsdir(self, recursive=True):
        return PathList(flatten(x.lsdir(recursive) for x in self if not x.is_dir()))

    def getinfo(self, sniff=False):
        return [x.getinfo(sniff) for x in self if x.is_file()]

    def wordcount(self, word, buf_size = 1024 ** 2):
        return [x.wordcount(word, buf_size) for x in self if x.is_file()]

    def linecount(self, buf_size = 1024 ** 2):
        return [x.linecount(buf_size) for x in self if x.is_file()]

    def geturi(self):
        return [x.geturi() for x in self if x.is_file()]

    def getsize(self):
        return [x.getsize() for x in self if x.is_file()]

    def gettype(self):
        return [x.gettype() for x in self if x.is_file()]

    def is_compress(self):
        return [x.is_compress() for x in self if x.is_file()]

    def __getattr__(self, name):
        if name in self.raiseop:
            raise RuntimeError("It's Operation is Dangerous. Please each Operation.")
        else:
            return [x.__getattribute__(name) for x in self if x.is_file()]

class Path(type(pathlib.Path())):
    SAMPLE = 92160

    __slots__ = (
        '_accessor',
        '_closed',
        '_encoding',
        '_ext',
        '_str',
        '_dialect',
        '_fullpath',
        'content',
    )

    def __new__(cls, *args, **kwargs):
        if args:
            content = None
            if isinstance(args[0], (cls, PathList)):
                return args[0]
            elif hasattr(args[0], "read") and hasattr(args[0], "name"):
                args = (args[0].name, *args[1:])
            elif isinstance(args[0], str):
                entity, content = path_norm(args[0])
                if re.search("[\*\[\]\?]", entity):
                    lst = []
                    for x in lsdir(entity, recursive=False):
                        x.content = content
                        lst.append(x)
                    if len(lst) == 1:
                        args = (lst[0], *args[1:])
                    elif len(lst) > 1:
                        return PathList(lst)
                    else:
                        args = (entity, *args[1:])
                else:
                    args = (entity, *args[1:])
        if cls is pathlib.Path:
            cls = pathlib.WindowsPath if os.name == 'nt' else pathlib.PosixPath
        self = cls._from_parts(args, init=False)
        if not self._flavour.is_supported:
            raise NotImplementedError("cannot instantiate {} on your system".forrmat(cls.__name__,))

        self.content = content
        self._init()
        return self

    def _init(self, *args, **kw):
        super()._init(*args, **kw)
        self._encoding = None
        self._ext = None
        self._dialect = None
        self._fullpath = None

    @property
    def fullpath(self):
        if self._fullpath is None:
            self._fullpath = self.joinpath(self.__str__(), self.content)
        return self._fullpath

    def getinfo(self, sniff=False):
        st = self.stat()
        pdir = self.parent

        ret = fifo(
                self,                             # fullpath
                pdir,                             # parent dir
                self.name,                        # basename
                self.suffix.lower(),              # extention
                isposix and self.owner() or None, # owner
                isposix and self.group() or None, # group
                oct(st.st_mode)[-3:],             # permision
                timestamp2date(st.st_ctime),      # create date
                timestamp2date(st.st_mtime),      # modified date
                st.st_size,                       # file size
                *[x or y for i, (x, y) in enumerate(zip_longest(dirs, pdir.parts)) if i < 10]          # directories hieralchy
        )
        if sniff:
            if self.is_dir():
                return fdifo(ret + kifo(None, "directory", None, None, None, None, None))
            else:
                return fkifo(ret + sniffer(self.read_bytes(91260)))
        else:
            return ret

    def read_bytes(self, n=-1):
        """
        Open the file in bytes mode, read it, and close the file.
        """
        with io.open(str(self), mode='rb') as f:
            return f.read(n)

    def read_text(self, n=-1, encoding=None, errors=None):
        """
        Open the file in text mode, read it, and close the file.
        """
        with io.open(str(self), mode='r', encoding=encoding or self.encoding, errors=errors) as f:
            return f.read(n)

    def delete(self):
        return self.unlink()

    def lsdir(self, recursive=True):
        return PathList(lsdir(self, recursive))

    def exists(self):
        try:
            return super().exists()
        except OSError:
            return False

    @property
    def encoding(self):
        if self._encoding is None:
            self._encoding = getencoding(self.read_bytes(self.SAMPLE))
        return self._encoding

    @property
    def ext(self):
        if self._ext is None:
            self._ext = self.gettype()
        return self._ext

    @property
    def dialect(self):
        if self._dialect is None:
            dat = self.read_bytes(self.SAMPLE)
            self._dialect = csv.Sniffer().sniff(dat.decode(self.encoding))
        return self._dialect

    @property
    def lineterminator(self):
        return self.dialect.lineterminator

    @property
    def quoting(self):
        return self.dialect.quoting

    @property
    def doublequote(self):
        return self.dialect.doublequote

    @property
    def delimiter(self):
        return self.dialect.delimiter
    sep = delimiter

    @property
    def quotechar(self):
        return self.dialect.quotechar

    def wordcount(self, word, buf_size = 1024 ** 2):
        with self.open(encoding=self.encoding) as f:
            read_f = f.read # loop optimization
            if isinstance(word, bytes):
                word = word.decode(self.encoding)
            elif isinstance(word, (int, float)):
                word = str(word)

            pos = f.tell()
            if pos != 0:
                f.seek(0)

            buf = read_f(buf_size)

            words = 0
            while buf:
                words += buf.count(word)
                buf = read_f(buf_size)

            f.seek(pos)
            return words

    def linecount(self, buf_size = 1024 ** 2):
        return self.wordcount(word=b"\n", buf_size = buf_size)

    def geturi(self):
        return geturi(str(self))

    def getsize(self):
        return self.stat().st_size

    def gettype(self):
        mime, ex = guess_type(self.name)
        et = self.suffix.lower()
        if not mime and not ex:
            return et
        elif mime and not ex:
            if et in guess_all_extensions(mime):
                return et
            else:
                return guess_extension(mime)
        elif mime and ex == "gzip":
            return guess_extension(mime) + ".gz"
        elif not mime and ex == "gzip":
            return ".gz"
        else:
            raise RuntimeError("Unknown Format " + ex)

    def tree_file(self, recursive:bool=True, dotfile:bool=False):
        if dotfile:
            for r in self.lsdir(recursive):
                if r.is_file():
                    yield r
        else:
            for r in self.lsdir(recursive):
                if not r.is_file() or any(x.startswith(".") for x in r.parts):
                    continue
                yield r

    def tree_dir(self, recursive:bool=True, dotfile:bool=False):
        if dotfile:
            for r in self.lsdir(recursive):
                if r.is_dir():
                    yield r
        else:
            for r in self.lsdir(recursive):
                if not r.is_dir() or any(x.startswith(".") for x in r.parts):
                    continue
                yield r

    def is_compress(self):
        if self.is_dir():
            return False
        return is_compress(self.read_bytes(265))

    def open(self, mode='r', buffering=-1, encoding=None,
             errors=None, newline=None):
        if self._closed:
            self._raise_closed()

        if self.is_compress():
            lst = list(zopen_recursive(self._str, mode))
            if len(lst) == 1:
                return lst[0]
            return PathList(lst)
        else:
            if "b" in mode:
                return io.open(str(self), mode, buffering=buffering, errors=errors, newline=newline)
            else:
                return io.open(str(self), mode, buffering, encoding or self.encoding, errors, newline)


class _baseArchive(object):
    __slots__ = (
        "parent",
        "info",
        "_opened",
        "_parentname",
        "_name",
        "_parentpath",
        "_dialect",
        "_fullpath",
        "_parent",
        "_basename",
        "_extention",
        "_owner",
        "_group",
        "_permision",
        "_cdate",
        "_mdate",
        "_filesize",
        "_encoding",
        "_lineterminator",
        "_quoting",
        "_doublequote",
        "_delimiter",
        "_quotechar"
    )

    def __init__(self, archived_object, info=None):
        self.parent = archived_object
        self.info = info
        self._opened = None
        self._parentname = None
        self._name = None
        self._dialect = None
        self._fullpath = None
        self._parentpath = None
        self._basename = None
        self._extention = None
        self._owner = None
        self._group = None
        self._permision = None
        self._cdate = None
        self._mdate = None
        self._filesize = None
        self._encoding = None
        self._lineterminator = None
        self._quoting = None
        self._doublequote = None
        self._delimiter = None
        self._quotechar = None

    @property
    def opened(self):
        if self._opened is None:
            self._opened = self.parent.open(self.info)
        return self._opened

    @property
    def parentname(self):
        if self._parentname is None:
            self._parentname = Path(self.parent.filename)
        return self._parentname

    @property
    def name(self):
        if self._name is None:
            self._name = self.info.filename
        return self._name

    @property
    def ext(self):
        return self.parentname.suffix.lower()

    @property
    def dialect(self):
        if self._dialect is None:
            tell = self.opened.tell()
            if tell != 0:
                self.opened.seek(0)
            self._dialect = getdialect(self.opened.read(91260))
            self.opened.seek(tell)
        return self._dialect

    @property
    def fullpath(self):
        if self._fullpath is None:
            self._fullpath = self.parentname.joinpath(self.name)
        return self._fullpath

    @property
    def parentpath(self):
        if self._parentpath is None:
            self._parentpath = self.fullpath.parent
        return self._parentpath

    @property
    def basename(self):
        if self._basename is None:
            self._basename = self.fullpath.name
        return self._basename

    @property
    def extention(self):
        if self._extention is None:
            self._extention = self.fullpath.suffix.lower()
        return self._extention

    @property
    def owner(self):
        pass

    @property
    def group(self):
        pass

    @property
    def permision(self):
        pass

    @property
    def cdate(self):
        pass

    @property
    def mdate(self):
        if self._mdate is None:
            self._mdate = dt(*self.info.date_time)
        return self._mdate

    @property
    def filesize(self):
        if self._filesize is None:
            self._filesize = self.info.file_size
        return self._filesize

    @property
    def encoding(self):
        tell = self.opened.tell()
        if tell != 0:
            self.opened.seek(0)
        self._encoding = getencoding(self.opened.read(91260))
        self.opened.seek(tell)
        return self._encoding

    @property
    def lineterminator(self):
        return self.dialect.lineterminator

    @property
    def quoting(self):
        return self.dialect.quoting

    @property
    def doublequote(self):
        return self.dialect.doublequote

    @property
    def delimiter(self):
        return self.dialect.delimiter
    sep = delimiter

    @property
    def quotechar(self):
        return self.dialect.quotechar

    def getinfo(self, sniff=False):
        ret = fifo(
                self.fullpath,
                self.parentpath,
                self.name,
                self.extention,
                self.owner,
                self.group,
                self.permision,
                self.cdate,
                self.mdate,
                self.filesize,
                *[x or y for i, (x, y) in enumerate(zip_longest(dirs, self.parentpath.parts)) if i < 10]
        )

        if sniff:
            if self.is_dir():
                return fdifo(ret + kifo(None, "directory", None, None, None, None, None))
            else:
                pos = self.opened.tell()
                self.opened.seek(0)
                ret = fkifo(*(ret + sniffer(self.read_bytes(91260))))
                self.opened.seek(pos)

        return ret

    def wordcount(self, word, buf_size = 1024 ** 2):
        read_f = self.read_text # loop optimization
        if isinstance(word, bytes):
            word = word.decode(self.encoding)
        elif isinstance(word, (int, float)):
            word = str(word)

        pos = self.tell()
        if pos != 0:
            self.opened.seek(0)
        buf = read_f(buf_size)

        words = 0

        while buf:
            words += buf.count(word)
            buf = read_f(buf_size)

        self.opened.seek(pos)
        return words

    def linecount(self, buf_size = 1024 ** 2):
        return self.wordcount(word=b"\n", buf_size = buf_size)

    def geturi(self):
        return geturi(self.fullpath)

    def getsize(self):
        return self.filesize

    def gettype(self):
        return self.extention[1:]

    def read_bytes(self, n=-1):
        return self.opened.read(n)

    def read_text(self, size=-1, encoding=None):
        e = encoding or self.encoding
        return self.opened.read(size).decode(e)

    def extract(self, path=None, *args, **kw):
        if path is None:
            path = self.parentname.parent
        return self.parent.extract(self.info, str(path), *args, **kw)

    def __dir__(self):
        return sorted(set(dir(self.opened) + dir(self.info) + dir(self.parent)))

    def __getattr__(self, val):
        if hasattr(self.opened, val):
            return self.opened.__getattribute__(val)
        if self.info and hasattr(self.info, val):
            return self.info.__getattribute__(val)
        if self.parent and hasattr(self.parent, val):
            return self.parent.__getattribute__(val)
        super().__getattribute__(val)

    def __iter__(self):
        return self.opened.__iter__()

    def __repr__(self):
        return "<class ArchiveFile opened={}, info={}, parent={}>".format(
                         self.opened, self.info, self.parent)


class ZipArchiveWraper(_baseArchive):
    def is_file(self):
        return not self.info.is_dir()


class TarArchiveWraper(_baseArchive):
    def __init__(self, archived_object, info=None):
        super().__init__(archived_object=archived_object, info=info)
        self.is_dir = self.info.isdir
        self.is_file = self.info.isfile

    @property
    def opened(self):
        if self._opened is None:
            self._opened = self.parent.extractfile(self.info)
        return self._opened

    @property
    def parentname(self):
        if self._parentname is None:
            self._parentname = Path(self.parent.name)
        return self._parentname

    @property
    def name(self):
        if self._name is None:
            self._name = self.info.name
        return self._name

    @property
    def owner(self):
        if self._owner is None:
            self._owner = self.info.uname or None
        return self._owner

    @property
    def group(self):
        if self._group is None:
            self._group = self.info.gname or None
        return self._group

    @property
    def permision(self):
        if self._permision is None:
            self._permision = oct(self.info.mode)[-3:]
        return self._permision

    @property
    def mdate(self):
        if self._mdate is None:
            self._mdate = dt.fromtimestamp(self.info.mtime)
        return self._mdate

    @property
    def filesize(self):
        if self._filesize is None:
            self._filesize = self.info.size
        return self._filesize

    def gettype(self):
        return Path(self.info.name).suffix.lower()[1:]


class LhaArchiveWraper(_baseArchive):
    @property
    def mdate(self):
        if self._mdate is None:
            self._mdate = self.info.date_time
        return self._mdate

    def extract(self, path=None, *args, **kw):
        """
        path: extract to directory (default: lzh file same directory)
        """
        if path is None:
            path = self.parentname.parent.joinpath(self.name)
        else:
            path = Path(path).joinpath(self.name)
        with binopen(path, "wb") as w:
            w.write(self.opened.read())
        return path

class RarArchiveWraper(_baseArchive):
    @property
    def parentname(self):
        if self._parentname is None:
            self._parentname = Path(self.parent._rarfile)
        return self._parentname

    @property
    def name(self):
        if self._name is None:
            self._name = self.info.filename
        return self._name


class ZLibArchiveWraper(_baseArchive):
    @property
    def opened(self):
        if self._opened is None:
            self._opened = self.parent
        return self._opened

    @property
    def parentname(self):
        if self._parentname is None:
            self._parentname = Path(self.info)
        return self._parentname

    @property
    def name(self):
        if self._name is None:
            self._name = self.parentname.stem
        return self._name

    @property
    def mdate(self):
        z = self.opened
        if self._mdate is None:
            self._mdate = hasattr(z, "mtime") and z.mtime and dt.fromtimestamp(z.mtime) or None
        return self._mdate

    @property
    def filesize(self):
        if self._filesize is None:
            self._filesize = getsize(self.opened)
        return self._filesize

    def extract(self, path=None, *args, **kw):
        if path is None:
            path = self.parentname.parent.joinpath(self.name)
        else:
            path = Path(path).joinpath(self.name)
        with binopen(path, "wb") as w:
            w.write(self.opened.read())
        return path

    def is_dir(self):
        return False
    def is_file(self):
        return True


if int("{}{}{}".format(*sys.version_info[:3])) <= 366:
    class ZipExtFile(zipfile.ZipExtFile):
        def __init__(self, fileobj, mode, zipinfo, decrypter=None,
                     close_fileobj=False):

            if not hasattr(fileobj, "seekable"):
                fileobj = fileobj._file

            super().__init__(fileobj, mode, zipinfo, decrypter=None,
                     close_fileobj=False)

            self._seekable = False

            try:
                if fileobj.seekable():
                    self._orig_compress_start = fileobj.tell()
                    self._orig_compress_size = zipinfo.compress_size
                    self._orig_file_size = zipinfo.file_size
                    self._orig_start_crc = self._running_crc
                    self._seekable = True
            except AttributeError:
                pass

        def seekable(self):
            return self._seekable

        def seek(self, offset, whence=0):
            if not self._seekable:
                raise io.UnsupportedOperation("underlying stream is not seekable")
            curr_pos = self.tell()
            if whence == 0: # Seek from start of file
                new_pos = offset
            elif whence == 1: # Seek from current position
                new_pos = curr_pos + offset
            elif whence == 2: # Seek from EOF
                new_pos = self._orig_file_size + offset
            else:
                raise ValueError("whence must be os.SEEK_SET (0), "
                                 "os.SEEK_CUR (1), or os.SEEK_END (2)")

            if new_pos > self._orig_file_size:
                new_pos = self._orig_file_size

            if new_pos < 0:
                new_pos = 0

            read_offset = new_pos - curr_pos
            buff_offset = read_offset + self._offset

            if buff_offset >= 0 and buff_offset < len(self._readbuffer):
                # Just move the _offset index if the new position is in the _readbuffer
                self._offset = buff_offset
                read_offset = 0
            elif read_offset < 0:
                # Position is before the current position. Reset the ZipExtFile
                self._fileobj.seek(self._orig_compress_start)
                self._running_crc = self._orig_start_crc
                self._compress_left = self._orig_compress_size
                self._left = self._orig_file_size
                self._readbuffer = b''
                self._offset = 0
                self._decompressor = zipfile._get_decompressor(self._compress_type)
                self._eof = False
                read_offset = new_pos

            while read_offset > 0:
                read_len = min(self.MAX_SEEK_READ, read_offset)
                self.read(read_len)
                read_offset -= read_len

            return self.tell()

        def tell(self):
            if not self._seekable:
                raise io.UnsupportedOperation("underlying stream is not seekable")
            filepos = self._orig_file_size - self._left - len(self._readbuffer) + self._offset
            return filepos
else:
    class ZipExtFile(zipfile.ZipExtFile):
        def __init__(self, fileobj, mode, zipinfo, decrypter=None,
                     close_fileobj=False):

            if not hasattr(fileobj, "seekable"):
                fileobj = fileobj._file

            super().__init__(fileobj, mode, zipinfo, decrypter=None,
                     close_fileobj=False)

zipfile.ZipExtFile = ZipExtFile

class _baseFile:
    def lsdir(self, recursive=True):
        for r in self.__iter__(): #TODO recursive option
            yield r

    def tree_file(self, recursive:bool=True, dotfile:bool=False):
        if dotfile:
            for r in self.lsdir(recursive):
                if r.is_file():
                    yield r
        else:
            for r in self.lsdir(recursive):
                p = pathlib.Path(r.name)
                if not r.is_file() or any(x.startswith(".") for x in p.parts):
                    continue
                yield r

    def tree_dir(self, recursive:bool=True, dotfile:bool=False):
        if dotfile:
            for r in self.lsdir(recursive):
                if r.is_dir():
                    yield r
        else:
            for r in self.lsdir(recursive):
                if not r.is_dir() or any(x.startswith(".") for x in r.parts):
                    continue
                yield r

class ZipFile(zipfile.ZipFile, _baseFile):
    def __init__(self, file, mode='r', compression=zipfile.ZIP_STORED, allowZip64=True):
        self.wildcard = None
        if isinstance(file, (str, Path)):
            file, self.wildcard = path_norm(str(file))

        super().__init__(file, mode=mode,
                compression=compression,
                allowZip64=allowZip64)

    def __iter__(self):
        for x in self.infolist():
            r = ZipArchiveWraper(self, x)
            if not self.wildcard or in_glob([r.filename], self.wildcard):
                yield r

    """ localize override
    customized reason for filename encoding cp437 -> cp932
    """
    def _RealGetContents(self):
        fp = self.fp
        try:
            endrec = zipfile._EndRecData(fp)
        except OSError:
            raise zipfile.BadZipFile("File is not a zip file")
        if not endrec:
            raise zipfile.BadZipFile("File is not a zip file")
        if self.debug > 1:
            print(endrec)
        size_cd = endrec[zipfile._ECD_SIZE]             # bytes in central directory
        offset_cd = endrec[zipfile._ECD_OFFSET]         # offset of central directory
        self._comment = endrec[zipfile._ECD_COMMENT]    # archive comment

        concat = endrec[zipfile._ECD_LOCATION] - size_cd - offset_cd
        if endrec[zipfile._ECD_SIGNATURE] == zipfile.stringEndArchive64:
            concat -= (zipfile.sizeEndCentDir64 + zipfile.sizeEndCentDir64Locator)

        if self.debug > 2:
            inferred = concat + offset_cd
            print("given, inferred, offset", offset_cd, inferred, concat)
        self.start_dir = offset_cd + concat
        fp.seek(self.start_dir, 0)
        data = fp.read(size_cd)
        fp = io.BytesIO(data)
        total = 0
        while total < size_cd:
            centdir = fp.read(zipfile.sizeCentralDir)
            if len(centdir) != zipfile.sizeCentralDir:
                raise zipfile.BadZipFile("Truncated central directory")
            centdir = zipfile.struct.unpack(zipfile.structCentralDir, centdir)
            if centdir[zipfile._CD_SIGNATURE] != zipfile.stringCentralDir:
                raise zipfile.BadZipFile("Bad magic number for central directory")
            if self.debug > 2:
                print(centdir)
            filename = fp.read(centdir[zipfile._CD_FILENAME_LENGTH])
            flags = centdir[5]
            if flags & 0x800:
                filename = filename.decode('utf-8')
            else:
                filename = filename.decode('cp932') #customize v3.6.6:4cf1f54eb7 base
            x = zipfile.ZipInfo(filename)
            x.extra = fp.read(centdir[zipfile._CD_EXTRA_FIELD_LENGTH])
            x.comment = fp.read(centdir[zipfile._CD_COMMENT_LENGTH])
            x.header_offset = centdir[zipfile._CD_LOCAL_HEADER_OFFSET]
            (x.create_version, x.create_system, x.extract_version, x.reserved,
             x.flag_bits, x.compress_type, t, d,
             x.CRC, x.compress_size, x.file_size) = centdir[1:12]
            if x.extract_version > zipfile.MAX_EXTRACT_VERSION:
                raise NotImplementedError("zip file version %.1f" %
                                          (x.extract_version / 10))
            x.volume, x.internal_attr, x.external_attr = centdir[15:18]
            x._raw_time = t
            x.date_time = ( (d>>9)+1980, (d>>5)&0xF, d&0x1F,
                            t>>11, (t>>5)&0x3F, (t&0x1F) * 2 )

            x._decodeExtra()
            x.header_offset = x.header_offset + concat
            self.filelist.append(x)
            self.NameToInfo[x.filename] = x

            total = (total + zipfile.sizeCentralDir + centdir[zipfile._CD_FILENAME_LENGTH]
                     + centdir[zipfile._CD_EXTRA_FIELD_LENGTH]
                     + centdir[zipfile._CD_COMMENT_LENGTH])

            if self.debug > 2:
                print("total", total)

    def open(self, name, mode="r", pwd=None, *, force_zip64=False):
        if mode not in {"r", "w"}:
            raise ValueError('open() requires mode "r" or "w"')
        if pwd and not isinstance(pwd, bytes):
            raise TypeError("pwd: expected bytes, got %s" % type(pwd).__name__)
        if pwd and (mode == "w"):
            raise ValueError("pwd is only supported for reading files")
        if not self.fp:
            raise ValueError(
                "Attempt to use ZIP archive that was already closed")

        if isinstance(name, zipfile.ZipInfo):
            zinfo = name
        elif mode == 'w':
            zinfo = zipfile.ZipInfo(name)
            zinfo.compress_type = self.compression
        else:
            zinfo = self.getinfo(name)

        if mode == 'w':
            return self._open_to_write(zinfo, force_zip64=force_zip64)

        if self._writing:
            raise ValueError("Can't read from the ZIP file while there "
                    "is an open writing handle on it. "
                    "Close the writing handle before trying to read.")

        self._fileRefCnt += 1
        zef_file = zipfile._SharedFile(self.fp, zinfo.header_offset,
                               self._fpclose, self._lock, lambda: self._writing)
        try:
            fheader = zef_file.read(zipfile.sizeFileHeader)
            if len(fheader) != zipfile.sizeFileHeader:
                raise zipfile.BadZipFile("Truncated file header")
            fheader = zipfile.struct.unpack(zipfile.structFileHeader, fheader)
            if fheader[zipfile._FH_SIGNATURE] != zipfile.stringFileHeader:
                raise zipfile.BadZipFile("Bad magic number for file header")

            fname = zef_file.read(fheader[zipfile._FH_FILENAME_LENGTH])
            if fheader[zipfile._FH_EXTRA_FIELD_LENGTH]:
                zef_file.read(fheader[zipfile._FH_EXTRA_FIELD_LENGTH])

            if zinfo.flag_bits & 0x20:
                raise NotImplementedError("compressed patched data (flag bit 5)")

            if zinfo.flag_bits & 0x40:
                raise NotImplementedError("strong encryption (flag bit 6)")

            if zinfo.flag_bits & 0x800:
                fname_str = fname.decode("utf-8")
            else:
                fname_str = fname.decode("cp932") #customize v3.6.6:4cf1f54eb7 base

            if fname_str != zinfo.orig_filename:
                raise zipfile.BadZipFile(
                    'File name in directory %r and header %r differ.'
                    % (zinfo.orig_filename, fname))

            # check for encrypted flag & handle password
            is_encrypted = zinfo.flag_bits & 0x1
            zd = None
            if is_encrypted:
                if not pwd:
                    pwd = self.pwd
                if not pwd:
                    raise RuntimeError("File %r is encrypted, password "
                                       "required for extraction" % name)

                zd = zipfile._ZipDecrypter(pwd)
                header = zef_file.read(12)
                h = list(map(zd, header[0:12]))
                if zinfo.flag_bits & 0x8:
                    check_byte = (zinfo._raw_time >> 8) & 0xff
                else:
                    check_byte = (zinfo.CRC >> 24) & 0xff
                if h[11] != check_byte:
                    raise RuntimeError("Bad password for file %r" % name)

            return ZipExtFile(zef_file, mode, zinfo, zd, True)
        except:
            zef_file.close()
            raise

class TarFile(tarfile.TarFile, _baseFile):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.infolist = self.getmembers
        self.namelist = self.getnames

    @classmethod
    def open(cls, name=None, mode='r', fileobj=None, *args, **kw):
        if not isinstance(name, (str, bytes)):
            fileobj = name
            name = None

        if isinstance(name, (str, Path)):
            name, wildcard = path_norm(str(name))

        ret = super().open(name=name, mode=mode, fileobj=fileobj, *args, **kw)
        ret.wildcard = wildcard
        return ret

    def __iter__(self):
        for x in self.getmembers():
            r = TarArchiveWraper(self, x)
            if not self.wildcard or in_glob([r.name], self.wildcard):
                yield r


TarFileOpen = TarFile.open

try:
    import rarfile

    rarfile.RarInfo.is_dir = rarfile.RarInfo.isdir
    rarfile.RarInfo.is_file = lambda _self: not _self.isdir()

    class RarFile(rarfile.RarFile, _baseFile):
        def __init__(self, file, mode="r", charset=None, info_callback=None,
                     crc_check=True, errors="stop"):
            self.wildcard = None
            if isinstance(file, (str, Path)):
                file, self.wildcard = path_norm(str(file))

            super().__init__(file, mode=mode, charset=charset,
                 info_callback=info_callback, crc_check=crc_check, errors=errors)

        def __iter__(self):
            for x in self.infolist():
                r = RarArchiveWraper(self, x)
                if not self.wildcard or in_glob([r.filename], self.wildcard):
                    yield r

        def extract(self, member, path=None, pwd=None):
            try:
                super().extract(member, path=path, pwd=pwd)
            except rarfile.RarCannotExec:
                if path is None:
                    path = Path(self._rarfile).parent
                else:
                    path = Path(path)

                if hasattr(member, "filename"):
                    path = path.joinpath(member.filename)
                elif hasattr(member, "name"):
                    path = path.joinpath(member.name)
                else:
                    path = path.joinpath(member)

                with open(path, "wb") as f:
                    f.write(self.read(member))
                return path

except ModuleNotFoundError:
    sys.stderr.write("** No module warning **\nPlease Install command: pip3 install rarfile\n")
    RarFile = ModuleNotFoundError

try:
    import lhafile


    class LhaInfo(lhafile.lhafile.LhaInfo):
        def is_dir(self):
            return self.directory is not None

        def is_file(self):
            return self.directory is None

    lhafile.lhafile.LhaInfo = LhaInfo

    class LhaFile(lhafile.LhaFile, _baseFile):
        def __init__(self, file, mode="r", compression=None,
                     callback=None, args=None):
            """ Open the LZH file """
            self.wildcard = None
            if isinstance(file, (str, Path)):
                file, self.wildcard = path_norm(str(file))

            super().__init__(file, mode=mode, compression=compression,
                 callback=callback, args=args)


        def open(self, name):
            """Return file bytes (as a string) for 'name'. """
            if not self.fp:
                raise RuntimeError("Attempt to read LZH archive that was already closed")
            if isinstance(name, LhaInfo):
                info = name
            else:
                info = self.NameToInfo[name]

            if info.compress_type in lhafile.Lhafile.SUPPORTED_COMPRESS_TYPE:
                self.fp.seek(info.file_offset)
                fin = lhafile.BytesOrStringIO(self.fp.read(info.compress_size))
                self.fout = lhafile.BytesOrStringIO()
                try:
                    session = lhafile.lzhlib.LZHDecodeSession(fin, self.fout, info)
                    while not session.do_next():
                        pass
                    outsize = session.output_pos
                    crc = session.crc16
                except Exception as e:
                    raise e
                if outsize != info.file_size:
                    raise lhafile.BadLhafile("%s output_size is not matched %d/%d %s" % \
                        (name, outsize, info.file_size, info.compress_type))
                if crc != info.CRC:
                    raise lhafile.BadLhafile("crc is not matched")

                self.fout.seek(0)
            elif info.commpress_type == '-lhd-':
                raise RuntimeError("name is directory")
            else:
                raise RuntimeError("Unsupport format")
            return self.fout

        @property
        def closed(self):
            return self.fp.closed

        def close(self):
            if hasattr(self, "fout") and self.fout.closed is False:
                self.fout.close()
            self.fp.close()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, tb):
            if exc_type:
                sys.stderr.write("{}\n{}\n{}".format(exc_type, exc_value, tb))
            self.close()
        def __iter__(self):
            for x in self.infolist():
                r = LhaArchiveWraper(self, x)
                if not self.wildcard or in_glob([r.filename], self.wildcard):
                    yield r

except ModuleNotFoundError:
    sys.stderr.write("** No module warning **\nPlease Install command: pip3 install lhafile\n")
    LhaFile = ModuleNotFoundError


class GzipFile(gzip.GzipFile, _baseFile):
    def __init__(self, filename=None, mode=None,
                 compresslevel=9, fileobj=None, mtime=None):
        if not isinstance(filename, (str, bytes)):
            fileobj = filename
            filename = None

        if isinstance(filename, (str, Path)):
            filename, self.wildcard = path_norm(str(filename))

        super().__init__(filename=filename, mode=mode,
                    compresslevel=compresslevel, fileobj=fileobj, mtime=mtime)

    def __iter__(self):
        yield ZLibArchiveWraper(self, self.name)

class LZMAFile(lzma.LZMAFile, _baseFile):
    def __init__(self, filename=None, mode="r", *,
                 format=None, check=-1, preset=None, filters=None):

        if isinstance(filename, (str, Path)):
            filename, self.wildcard = path_norm(str(filename))

        super().__init__(filename=filename, mode=mode,
                 format=format, check=check, preset=preset, filters=filters)

        self.name = self._fp.name

    def __iter__(self):
        yield ZLibArchiveWraper(self, self.name)

class BZ2File(bz2.BZ2File, _baseFile):
    def __init__(self, filename, mode="r", buffering=None, compresslevel=9):
        if isinstance(filename, (str, Path)):
            filename, self.wildcard = path_norm(str(filename))

        super().__init__(filename, mode=mode, buffering=buffering, compresslevel=compresslevel)

        self.name = self._fp.name

    def __iter__(self):
        yield ZLibArchiveWraper(self, self.name)


class _handler_zopen:
    archived_magic_numbers = [
        (b"\x1f\x8b", ".gz", GzipFile),
        (b"ustar\x00", ".tar", TarFileOpen),
        (b"ustar\x40", ".tar", TarFileOpen),
        (b"ustar  \x00", ".tar", TarFileOpen),
        (b"ustar  \x40", ".tar", TarFileOpen),
        (b"PK\x03\x04", ".zip", ZipFile),
        (b"PK\x05\x06", ".zip", ZipFile),
        (b"PK\x07\x08", ".zip", ZipFile),
        (b"BZh", ".bz2", BZ2File),
        (b"!\xd1-lh0-", ".lzh", LhaFile),
        (b"!\xd1-lh1-", ".lzh", LhaFile),
        (b"!\xd1-lh4-", ".lzh", LhaFile),
        (b"!\xd1-lh5-", ".lzh", LhaFile),
        (b"!\xd1-lh6-", ".lzh", LhaFile),
        (b"!\xd1-lh7-", ".lzh", LhaFile),
        (b"!\x82-lh0-", ".lzh", LhaFile),
        (b"!\x82-lh1-", ".lzh", LhaFile),
        (b"!\x82-lh4-", ".lzh", LhaFile),
        (b"!\x82-lh5-", ".lzh", LhaFile),
        (b"!\x82-lh6-", ".lzh", LhaFile),
        (b"!\x82-lh7-", ".lzh", LhaFile),
        (b"\x1f\x9d", ".Z", ""), #CompressZFile
        (b"\xFD7zXZ\x00", ".xz", LZMAFile),
        (b"7zBCAF271C", ".7z", LZMAFile),
        (b"RE\x7e\x5e", ".rar", RarFile),
        (b"Rar!\x1A\x07\x00", ".rar", RarFile),
        (b"Rar!\x1A\x07\x01\x00(RAR5)", ".rar", RarFile),
        (b"LZIP", ".lz", LZMAFile),
        (b"MSCF\x00\x00\x00\x00", ".cab", ZipFile),
        (b"DGCA", ".dgc", ""),
        (b"GCAX", ".gca", ""),
    ]

    magic_header = max([265, max(len(x[0]) for x in archived_magic_numbers)])

    def __new__(cls, path_or_buffer):
        if isinstance(path_or_buffer, str) and os.path.exists(path_or_buffer):
            if os.path.isdir(path_or_buffer) or os.path.splitext(path_or_buffer)[-1] in [".xlsx", ".docx", ".pptx"]:
                raise NonCompressedError("`{}` is non Compressed file".format(path_or_buffer))

        if isinstance(path_or_buffer, bytes):
            b = BytesIO(path_or_buffer)
        else:
            b = binopen(path_or_buffer)

        pos = b.tell()
        if pos != 0:
            b.seek(0)

        dat = b.read(cls.magic_header)

        if hasattr(path_or_buffer, "close"):
            b.seek(pos)
        else:
            b.close()

        for magic, ext, opn in cls.archived_magic_numbers:
            if dat.startswith(magic):
                if b".tar\x00" in dat.lower() or b".tgz\x00" in dat.lower():
                    return TarFileOpen
                elif ext == "":
                    raise UnsupportCompressError("Sorry.\n{} is Unsupported...".format(ext[1:]))
                else:
                    return opn
            elif ext == ".tar" and dat[257:265].startswith(magic):
                return opn

        raise NonCompressedError("`{}` is non Compressed file".format(str(path_or_buffer)))

def is_compress(path_or_buffer):
    try:
        _handler_zopen(path_or_buffer)
        return True
    except NonCompressedError:
        return False
    except UnsupportCompressError:
        return True

def zopen(path_or_buffer, *args, **kw):
    return _handler_zopen(path_or_buffer)(path_or_buffer, *args, **kw)

def zopen_recursive(path_or_buffer, *args, **kw):
    z = zopen(path_or_buffer, *args, **kw)
    for ret in z:
        if is_compress(ret):
            for r in  zopen_recursive(ret.opened, *args, **kw):
                yield r
        else:
            yield ret

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

def test():

    def test_lsdir():
        def _testargs(func, pathstr, *args):
            #TODO assert
            ret = [
                func(pathstr, *args),
                func(pathlib.Path(pathstr), *args),
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


    def test_getsize():
        with open(tdir+"diff1.csv", "rb") as f:
            dat = f.read()
            f.seek(0)
            assert(getsize(f) == len(dat))

    def test_geturi():
        assert(geturi(tdir) == "file://" + (isposix is False and "/" or "") + tdir.replace("\\", "/")[:-1])
        Path(geturi(tdir+"test.zip"))

    def test_getdialect():
        with open(tdir+"diff1.csv", "rb") as f:
            assert(getdialect(f.read()).delimiter == ",")


    def test_sniffer():
        with open(tdir+"diff1.csv", "rb") as f:
            assert(sniffer(f.read()) == kifo(sep=',', encoding='cp932', lineterminator='\r\n', quoting=csv.QUOTE_MINIMAL, doublequote=False, delimiter=',', quotechar='"'))

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

    def test_Path():
        p = Path(tdir+"diff1.csv")
        assert(p.ext == ".csv")
        assert(p.encoding == "cp932")
        assert(p.dialect)
        assert(p.lineterminator == "\r\n")
        assert(p.delimiter == ",")
        assert(p.linecount() > 0)
        assert(p.wordcount(",") > 0)
        assert(len(list(p.lsdir())) == 1)
        assert(len(list(p.tree_file()))==1)
        assert(list(p.tree_dir()) == [])
        assert(p.is_compress() is False)
        assert(Path(open(p)) == p)
        
        p = Path(tdir)
        assert(p.ext == "")
        assert(len(list(p.lsdir())) > 0)
        assert(p.is_compress() is False)

        p = Path(tdir+"test.tar.gz")
        assert(p.is_compress() is True)
        assert(Path(open(p)) == p)

        p = Path(tdir+"test.zip/test.csv")
        assert(p.is_compress() == True)
        assert(isinstance(p.open(), ZipArchiveWraper))
        assert(p.read_bytes())
        assert(isinstance(Path(open(p)), Path))


        p = Path(tdir+"test.zip/test.csv")
        assert((p.as_posix(), p.content, p.fullpath.as_posix()) == (tdir+"test.zip", "test.csv", tdir+"test.zip/test.csv"))
        assert(p.exists() is True)
        assert(p.is_compress() is True)

        p = Path(tdir+"1test*.zip/test.csv")
        assert((p.as_posix(), p.content, p.fullpath.as_posix()) == (tdir+"1test*.zip", "test.csv", tdir+"1test*.zip/test.csv"))
        assert(p.exists() is False)
        
        p = Path(tdir+"test*.zip/test.csv")
        assert(p.is_compress() is True)

    def test_PathList():
        p = Path(tdir+"diff*")
        assert(len(p) == 4)
        assert(all(isinstance(x, Path) for x in p))
        p = Path(tdir+"diff*.csv")
        assert(p.sep == [",", ","])

        try:
            p.mkdir
        except RuntimeError:
            pass
        except:
            raise AssertionError
        
        try:
            p.replace
        except RuntimeError:
            pass
        except:
            raise AssertionError

        try:
            p.rmdir
        except RuntimeError:
            pass
        except:
            raise AssertionError
        
        assert(p.encoding == ["cp932", "cp932"])
        assert(p.ext == [".csv", ".csv"])
        assert(p.lineterminator == ["\r\n", "\r\n"])
        assert(p.quoting == [0,0])
        assert(p.doublequote == [False, False])
        assert(p.delimiter == [",", ","])
        assert(p.quotechar == ['"', '"'])
        
        p = Path(tdir+"test.zip/*")
        assert(isinstance(p, Path))
        

    def test_binopen():
        pass

    def test_opener():

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

    def test_compute_object_size():
        assert(compute_object_size(_handler_zopen.archived_magic_numbers) > 0)

    def test_logger():
        s = StringIO(newline=None)
        with logger(s) as log:
            log.write("hoge")
            assert(log.getvalue() == "hoge")
            log.write("foo", 0)
            assert(log.getvalue() == "hoge")
            log.write("foo", 2)
            assert(log.getvalue() == "hogefoo")
            log.loglevel = 2
            log.write("bar")
            assert(log.getvalue() == "hogefoo")
            log.write("bar", 3)
            assert(log.getvalue() == "hogefoobar")
            log.loglevel = 1
            log.writelines(["123","456"])
            assert(log.getvalue() == "hogefoobar123456")
        assert(s.closed)

        s = StringIO(newline=None)
        with logger(s) as log:
            log("hoge")
            assert(log.getvalue() == "hoge")
            log("foo", 0)
            assert(log.getvalue() == "hoge")
            log("foo", 2)
            assert(log.getvalue() == "hogefoo")
            log.loglevel = 2
            log("bar")
            assert(log.getvalue() == "hogefoo")
            log("bar", 3)
            assert(log.getvalue() == "hogefoobar")
        assert(s.closed)

    def test_islarge():
        assert(islarge([]) is False)
        global BUF
        BUF = 10
        assert(islarge(_handler_zopen.archived_magic_numbers) is True)
        BUF = 128 * 1024 ** 2


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

    def test_isdataframe():
        import pandas as pd
        assert(isdataframe(pd.DataFrame()))
        assert(isdataframe([]) is False)
        assert(isdataframe({1:1}) is False)
        assert(isdataframe(pd.Series()) is False)

    def test_sortedrows():
        assert(list(sortedrows(iter([[3],[2],[5],[1]]))) == [(4, [1]), (2, [2]), (1, [3]), (3, [5])])
        assert(list(sortedrows(iter([[3],[2],[5],[1]]),start=0)) == [(3, [1]), (1, [2]), (0, [3]), (2, [5])])
        assert(list(sortedrows(iter([[3],[2],[5],[1]]))) == [(4, [1]), (2, [2]), (1, [3]), (3, [5])])
        assert(list(sortedrows(iter([[1,3],[2,2],[3,5],[4,1]]), lambda x: x[1][1])) == [(4, [4, 1]), (2, [2, 2]), (1, [1, 3]), (3, [3, 5])])
        assert(list(sortedrows(iter([[3],[2],[5],[1]]), start=0)) == [(3, [1]), (1, [2]), (0, [3]), (2, [5])])

    def test_iterrows():
        from util.dfutil import read_any
        a = iter(list("abc"))
        h = iterrows(a,None)
        assert(list(h) == ['a', 'b', 'c'])

        f = tdir + "diff1.csv"
        a = read_any(f).head(3)
        a.reset_index(inplace=True)
        idxcheck = [1] + a.index.tolist()
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

    def test_RarFile():
        with RarFile(tdir+"test.rar") as l:
            __test_wrap(l)

        with RarFile(tdir+"test.rar/test.*") as l:
            assert(len(list(l))>0)
            __test_wrap(l)


    def test_LhaFile():
        with LhaFile(tdir+"test.lzh") as l:
            __test_wrap(l)

        with LhaFile(tdir+"test.lzh/test.*") as l:
            __test_wrap(l)


    def test_ZipFile():
        with ZipFile(tdir+"test.zip") as l:
            __test_wrap(l)

        with ZipFile(tdir+"test.zip/test.*") as l:
            assert(len(list(l))>0)
            __test_wrap(l)


    def test_TarFileOpen():
        with TarFileOpen(tdir+"test.tar.gz") as l:
            __test_wrap(l)

        with TarFileOpen(tdir+"test.tar.gz/test.*") as l:
            assert(len(list(l))>0)
            __test_wrap(l)

    def test_GzipFile():
        with GzipFile(tdir+"test.csv.gz") as l:
            __test_wrap(l)

        with GzipFile(tdir+"test.csv.gz/test.*") as l:
            assert(len(list(l))>0)
            __test_wrap(l)


    def test_LZMAFile():
        with LZMAFile(tdir+"test.csv.xz") as l:
            __test_wrap(l)

        with LZMAFile(tdir+"test.csv.xz/test.*") as l:
            assert(len(list(l))>0)
            __test_wrap(l)


    def test_BZ2File():
        with BZ2File(tdir+"test.csv.bz2") as l:
            __test_wrap(l)

        with BZ2File(tdir+"test.csv.bz2/test.*") as l:
            assert(len(list(l))>0)
            __test_wrap(l)


    def __test_ArchiveWrapper(r):
        assert(r.parent)
        assert(r.info)
        assert(r.opened)
        assert(r.parentname)
        assert(r.name)
        assert(r.ext)
        assert(isinstance(r.dialect, type))
        assert(r.fullpath)
        assert(r.parentpath)
        assert(r.basename)
        assert(r.extention == ".csv")
        r.owner
        r.group
        r.permision
        r.mdate
        assert(r.filesize)
        assert(r.encoding == "cp932")
        assert(r.lineterminator == "\r\n")
        assert(r.quoting == 0)
        assert(r.doublequote is False)
        assert(r.delimiter == ",")
        assert(r.quotechar == '"')
        assert(isinstance(r.getinfo(), fifo))
        assert(r.wordcount("1") == 2)
        assert(r.wordcount(1) == 2)
        assert(r.wordcount("あ") == 1)
        assert(r.linecount() == 3)
        assert(r.geturi())
        assert(r.getsize())
        assert(r.gettype() == "csv")
        assert(r.read_bytes() == b'n,aa\r\n1,1\r\n2,\x82\xa0\r\n')
        r.seek(0)
        assert(r.read_text() == 'n,aa\r\n1,1\r\n2,あ\r\n')

    def test__baseArchive():
        class testoverride(_baseArchive):
            pass
        testoverride.__init__

    def test_ZipArchiveWraper():
        with zipfile.ZipFile(tdir+"test.zip") as l:
            r = ZipArchiveWraper(l, l.infolist()[0])
            __test_ArchiveWrapper(r)

    def test_TarArchiveWraper():
        with tarfile.open(tdir+"test.tar.gz") as l:
            r = TarArchiveWraper(l, l.getmembers()[0])
            __test_ArchiveWrapper(r)

    def test_LhaArchiveWraper():
        l = LhaFile(tdir+"test.lzh")
        r = LhaArchiveWraper(l, l.infolist()[0])
        __test_ArchiveWrapper(r)

    def test_RarArchiveWraper():
        with RarFile(tdir+"test.rar") as l:
            r = RarArchiveWraper(l, l.infolist()[0])
            __test_ArchiveWrapper(r)

    def test_ZLibArchiveWraper():
        with GzipFile(tdir+"test.csv.gz") as l:
            r = ZLibArchiveWraper(l, l.name)
            __test_ArchiveWrapper(r)

        with LZMAFile(tdir+"test.csv.xz") as l:
            r = ZLibArchiveWraper(l, l.name)
            __test_ArchiveWrapper(r)


        with BZ2File(tdir+"test.csv.bz2") as l:
            r = ZLibArchiveWraper(l, l.name)
            __test_ArchiveWrapper(r)


    def test__handler_zopen():
        assert(_handler_zopen(tdir+"test.zip") == ZipFile)
        assert(_handler_zopen(tdir+"test.tar.gz") == TarFileOpen)
        assert(_handler_zopen(tdir+"test.lzh") == LhaFile)
        assert(_handler_zopen(tdir+"test.rar") == RarFile)
        assert(_handler_zopen(tdir+"test.csv.gz") == GzipFile)
        assert(_handler_zopen(tdir+"test.csv.xz") == LZMAFile)
        assert(_handler_zopen(tdir+"test.csv.bz2") == BZ2File)
        try:
            _handler_zopen(tdir+"test.csv")
        except NonCompressedError:
            pass
        else:
            raise AssertionError


    def test_is_compress():
        assert(is_compress(tdir) is False)
        assert(is_compress(tdir + "test.csv.tar") is True)
        assert(is_compress(tdir + "diff1.csv") is False)
        assert(is_compress(tdir + "diff1.xlsx") is False)
        assert(is_compress(tdir + "sample.accdb") is False)
        assert(is_compress(tdir + "sample.sqlite3") is False)
        assert(is_compress(tdir + "test.csv") is False)
        assert(is_compress(tdir + "test.csv.bz2") is True)
        assert(is_compress(tdir + "test.csv.gz") is True)
        assert(is_compress(tdir + "test.csv.tar") is True)
        assert(is_compress(tdir + "test.csv.tar.gz") is True)
        assert(is_compress(tdir + "test.csv.xz") is True)
        assert(is_compress(tdir + "test.lzh") is True)
        assert(is_compress(tdir + "test.rar") is True)
        assert(is_compress(tdir + "test.tar.gz") is True)
        assert(is_compress(tdir + "test.zip") is True)
        try:
            is_compress("hoge")
        except FileNotFoundError:
            pass
        else:
            raise AssertionError


    def test_zopen():
        with zopen(tdir+"test.zip") as z:
            assert(isinstance(z, ZipFile))
        with zopen(tdir+"test.tar.gz") as z:
            assert(isinstance(z, TarFile))
        with zopen(tdir+"test.csv.gz") as z:
            assert(isinstance(z, GzipFile))
        with zopen(tdir+"test.csv.xz") as z:
            assert(isinstance(z, LZMAFile))
        with zopen(tdir+"test.csv.bz2") as z:
            assert(isinstance(z, BZ2File))
        with zopen(tdir+"test.lzh") as z:
            assert(isinstance(z, LhaFile))
        with zopen(tdir+"test.rar") as z:
            assert(isinstance(z, RarFile))

        try:
            with zopen(tdir+"test.csv") as z:
                assert(isinstance(z, TarFile))
        except NonCompressedError:
            pass
        else:
            raise AssertionError

        try:
            zopen(tdir+"notfound.csv")
        except FileNotFoundError:
            pass


    def test_zopen_recursive():
        for f in zopen_recursive(tdir+"test.csv.tar.gz"):
            assert(f.read() == b'n,aa\r\n1,1\r\n2,\x82\xa0\r\n')


    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            func()

if __name__ == "__main__":
    test()

