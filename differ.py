# -*- coding: utf-8 -*-
# author : m.yamagami

__version__ = '0.0.4'
import os
import sys
import csv
import re
from difflib import SequenceMatcher
from itertools import tee
from io import StringIO

try:
    from cytoolz.itertoolz import zip_longest
except:
    from itertools import zip_longest

#from itertools import zip_longest

emsg = """
## WARNING ##
    ** Please install module `pip install xsorted` **

"""

try:
    from xsorted import xsorted
except ImportError as e:
    sys.stderr.write(emsg)
    xsorted = sorted

def guess_sep(s):
    return csv.Sniffer().sniff(s).delimiter

def getencoding():
    d = dict(win32="cp932", linux="utf-8", cygwin="utf-8", darwin="utf-8")
    return d[sys.platform]

def getlinesep():
    d = dict(win32="\r\n", linux="\n", cygwin="\n", darwin="\n")
    return d[sys.platform]


def opener(f, encoding=None):
    if hasattr(f, "read"):
        return f
    if isinstance(f, str):
        if os.path.exists(os.path.dirname(f)):
            return open(f)
        else:
            raise ValueError("File not Found `{}`".format(f))

class logger(object):
    def __init__(self, filepath_or_buffer=None, autoclose=True):
        self.name = None
        self.autoclose = autoclose
        if filepath_or_buffer is None:
            self.con = sys.stdout
        elif isinstance(filepath_or_buffer, str) and os.path.exists(os.path.dirname(filepath_or_buffer)):
            self.name = filepath_or_buffer
            self.con = open(self.name, "w")
        elif isinstance(filepath_or_buffer, StringIO) or filepath_or_buffer == sys.stdout:
            self.con = filepath_or_buffer
        elif hasattr(filepath_or_buffer, "write"):
            self.con = filepath_or_buffer
            self.name = self.con.name
        else:
            raise AttributeError("unknown type logfilepath `{}`".format(filepath_or_buffer))
    def close(self):
        if hasattr(self.con, "close") and self.name and self.autoclose:
            self.con.close()
    def __getattr__(self,name):
        self.con.__getattr__(name)
    def __enter__(self):
        return self.con
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            sys.stderr.write("{}\n{}\n{}".format(exc_type, exc_value, traceback))
        self.close()


def pprint(a,b, title="", sep=",", linesep="\n", *arg, **kw):
    with StringIO() as sio:
        Differ(a,b, *arg, **kw).pprint(f=sio,title=title, sep=sep, linesep=linesep)
        return sio.getvalue()

def count(a,b,*arg, **kw):
    s = Differ(a,b, *arg, **kw)
    for d in s:
        pass
    return s.result

def is_same(a,b,*arg, **kw):
    s = Differ(a,b, *arg, **kw)
    for d in s:
        pass
    return s.is_same

_render_ = """
## {title} Summary  ##
equal  : {equal} line
replace: {replace} line
delete : {delete} line
insert : {insert} line

"""
def sumary(a,b,title="", *arg, **kw):
    s = Differ(a,b, *arg, **kw)
    return _render_.format(title=title, **s.result)

def items(a,b,*arg, **kw):
    for d in Differ(a,b, *arg, **kw):
        yield d

lf = re.compile(r"\r?\n")
rmlf = lambda x: lf.sub("", x)

class Differ(object):
    def __init__(self, a, b, skipequal=True, header=False, key=None, in_memory=True, isjunk=None):
        """
        Useful Diff Iterator
        
        Parameter: 
            a : (iteratable) diff target list1 require
            b : (iteratable) diff target list2 require
            skipequal : (boolean) do you wan't output same sequence? (default True is non same only)
            key : (1 arg callable) diff compare key function
            in_memory : (boolean) when very large data is `False` (default True)
            
        Example:
        >>> for tag, in Differ(a=list("abc"), b=list("aac")):
        ...            print(d)
        ('replace', 1, "b", 1, "a")
        
        Return: 
        tuple(tag, i, la, j, lb)
            tag : str('replace' or 'insert, or 'delete'  or 'equal')
            i      : index of a
            la    : side by a data
            j      : index of a
            lb    : side by b data
        """
        self.a = a
        self.b = b
        self.header = header
        self.key = key
        self.in_memory = in_memory
        self.skipequal = skipequal
        self.isjunk = isjunk
        self.result = dict(equal=0, replace=0, delete=0, insert=0)
        self.header_a = None
        self.header_b = None
    
    @property
    def sorter(self):
        if self.in_memory:
            return sorted
        else:
            if xsorted.__module__ is "builtins":
                raise ImportError(emsg)
                sys.exit(1)
            return xsorted
            
    def is_same(self):
        """
        check function
        Same or Different?
        
        Return boolean
        """
        return sum(self.result.values()) - self.result["equal"] == 0
    
    def detail(self, sep=",", linesep="\n", title=""): #TODO memory usage
        csv.register_dialect('user', delimiter=sep, lineterminator=linesep, quoting=csv.QUOTE_MINIMAL)
        ret = "## {} Detail  ##\n".format(title)
        ret += sep.join(["VALID","FILE1_LINENO","FILE1_LINENO", "SPLIT","[RESULT...]\n"])
        
        for tag,context,i,j in self.compare():
            if i is None:
                i = "-"
            else:
                i += 1
            if j is None:
                j = "-"
            else:
                j += 1

            ret += sep.join(str(x) for x in flatten([tag, i, j, "|", context])) + linesep
        return ret

    def sumary(self, title=""):
        return _render_.format(title=title, **self.result)

    def pprint(self, f=None, sep=",", linesep="\n", title=""):
        """
        Diff result log prety printing function
        Return:
            default STDOUT print
        """
        
        with logger(f) as w:
            w.write(self.detail(sep=sep, linesep=linesep, title=title))               
            w.write(self.sumary(title=title))

    def range(self, start, end):
        if self.header:
            return range(start+1, end+1)
        else:
            return range(start, end)

    def codes(self):
        if self.key:
            self.a = self.sorter(self.a, key=self.key)
            self.b = self.sorter(self.b, key=self.key)
        seq = SequenceMatcher(self.isjunk, self.a, self.b, autojunk=False)

        if self.header:
            tag = self.header_a == self.header_b and "equal" or "replace"
            self.result[tag] += 1
            yield tag, 0, self.header_a, 0, self.header_b
      
        for tag, i1, i2, j1, j2 in seq.get_opcodes():
            lon = zip_longest(self.range(i1,i2), self.a[i1:i2], self.range(j1,j2), self.b[j1:j2])
            for i, contexta, j, contextb in lon:
                if contexta == contextb: tag = "equal"
                elif not contexta: tag = "insert"
                elif not contextb: tag = "delete"
                else: tag = "replace"
                self.result[tag] += 1
                if self.skipequal and tag == "equal":
                    continue
                yield tag, i, contexta, j, contextb


    def _compare(self):
        if hasattr(self.b, "__next__"):
            if self.in_memory:
                self.a, self.b = list(self.a), list(self.b)
            else:
                self.a, self.b = _listlike(self.a), _listlike(self.b)
        
        if self.header:
            if self.header_a is None:
                if hasattr(self.a, "__next__"):
                    self.header_a = next(self.a)
                else:
                    self.header_a, self.a = self.a[0], self.a[1:]
            if self.header_b is None:
                if hasattr(self.b, "__next__"):
                    self.header_b = next(self.b)
                else:
                    self.header_b, self.b = self.b[0], self.b[1:]
        
        try:
            for tag,i,a,j,b in self.codes():
                yield tag, i, _norm(a), j, _norm(b) #TODO
        
        except TypeError:
            if self.header:
                self.header_a, self.header_b = repr(self.header_a), repr(self.header_b)
                
            self.a, self.b = [repr(y) for y in self.a], [repr(y) for y in self.b]
        
            for tag,i,a,j,b in self.codes():
                yield tag, i, a and eval(a) or [], j, b and eval(b) or []

    def compare(self):
        for tag,i,a,j,b in self._compare():
            if tag == "equal":
                yield tag,a,i,j
            if tag == "replace":
                yield tag, _comp(a,b),i,j #TODO
            if tag == "insert":
                yield tag,b,None,j
            if tag == "delete":
                yield tag,a,i, None

    def __iter__(self):
        return self._compare()

def _listlike(iterator):
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

            for i in range(k - cache_len + 1):
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

def _norm(x):
    if x is None:
        return []
    if isinstance(x, str) or isinstance(x, int):
        return [x]
    return x

def _comp(a, b):
    ret = []
    for aa, bb in zip_longest(a, b, fillvalue=""):
        if aa == bb:
            ret.append(aa)
        elif aa and not bb:
            ret.append("{} ---> DEL".format(aa))
        elif not aa and bb:
            ret.append("ADD ---> {}".format(bb))
        else:
            ret.append("{} ---> {}".format(aa, bb))
    return ret

def _parserow(x, dialect):
    if x is None:
        return []
    if isinstance(x, int):
        return [x]
    if isinstance(x, str):
        return list(csv.reader([x],dialect=dialect))[0]
    return x

def _rowcompare(a, b, dialect=None):
    ret = []
    if dialect is None:
        z = zip_longest(_norm(a), _norm(b), fillvalue="")
    else:
        z = zip_longest(_parserow(a, dialect), _parserow(b, dialect), fillvalue="")
    for aa, bb in z:
        if aa == bb:
            ret.append(aa)
        elif aa and not bb:
            ret.append("{} ---> DEL".format(aa))
        elif not aa and bb:
            ret.append("ADD ---> {}".format(bb))
        else:
            ret.append("{} ---> {}".format(aa, bb))
    return ret

def flatten(L):
    ret = []
    fr = list(L)

    while len(fr) > 0:
        n = fr.pop(0)
        if isinstance(n, list):
            fr = n + fr
        else:
            ret.append(n)
    return ret

def test():
    global a, b, anser, s, ss, s2
    a = [[1,2,3],[1,2,3]]
    b = [[1,2,3],[3,2,1]]
    
    assert list(Differ(a,b,skipequal=False)) == [('equal', 0, [1, 2, 3], 0, [1, 2, 3]), ('replace', 1, [1, 2, 3], 1, [3, 2, 1])]
    assert list(Differ(a,b,skipequal=True)) == [('replace', 1, [1, 2, 3], 1, [3, 2, 1])]
    assert list(Differ(a,b,skipequal=False,header=True)) == [('equal', 0, [1, 2, 3], 0, [1, 2, 3]), ('replace', 1, [1, 2, 3], 1, [3, 2, 1])]
    assert list(Differ(a,b,skipequal=True,header=True)) == [('equal', 0, [1, 2, 3], 0, [1, 2, 3]), ('replace', 1, [1, 2, 3], 1, [3, 2, 1])]
    
    anser = [('equal', 0, [1], 0, [1]), ('replace', 1, [1], 1, [2]), ('equal', 2, [3], 2, [3]), ('delete', 3, [4], None, [])]
    a = (x for x in (1,1,3,4))
    b = (x for x in (1,2,3))
#    print(list(Differ(a,b,skipequal=False)) is anser)
    assert list(Differ(a,b,skipequal=False)) == anser
    a = (1,1,3,4)
    b = (1,2,3)
    assert list(Differ(a,b,skipequal=False)) == anser
    a = [1,1,3,4]
    b = [1,2,3]
    assert list(Differ(a,b,skipequal=False)) == anser
    
    assert _rowcompare(a,b) == [1, '1 ---> 2', 3, '4 ---> DEL']
#    print(pprint(a,b))
    assert(pprint(a,b) == '##  Detail  ##\nVALID,FILE1_LINENO,FILE1_LINENO,SPLIT,[RESULT...]\nreplace,2,2,|,1 ---> 2\ndelete,4,-,|,4\n\n##  Summary  ##\nequal  : 2 line\nreplace: 1 line\ndelete : 1 line\ninsert : 0 line\n\n')

    s = Differ(a,b)
    ss = StringIO()
    s.pprint(f=ss)
    assert(ss.getvalue()=='##  Detail  ##\nVALID,FILE1_LINENO,FILE1_LINENO,SPLIT,[RESULT...]\nreplace,2,2,|,1 ---> 2\ndelete,4,-,|,4\n\n##  Summary  ##\nequal  : 2 line\nreplace: 1 line\ndelete : 1 line\ninsert : 0 line\n\n')
    
    
    b = _listlike(a)
    assert(b[3]==4)
    assert(b[3]==4)
    assert(b[0]==1)
    assert(b[0:3]==[1, 1, 3])
    assert(len(b)==4)
    assert(list(b)==[1, 1, 3, 4])


    # sort test    
    a = [[1,2,3],[1,2,3]]
    b = [[3,2,1],[1,2,3]]
    s2 = Differ(a,b,key=str)
    assert(list(s2.compare()) == [('replace', ['1 ---> 3', 2, '3 ---> 1'], 1, 1)])
    
    a,b = r"C:\temp\hoge_before.csv C:\temp\hoge_after.csv".split(" ")
    s=Differ(open(a), open(b),header=True)
#    print(list(s.compare()))
    assert(list(s.compare())[0] == ('equal', ['mpg,cyl,displ,hp,weight,accel,yr,origin,name\n'], 0, 0))
#    print(s.result)
    assert(s.result == {'equal': 386, 'replace': 3, 'delete': 2, 'insert': 1})
    

if __name__ == "__main__":
    test()
    
    
