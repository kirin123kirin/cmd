# -*- coding: utf-8 -*-
import os
import sys
import re
import csv
import heapq
from tempfile import NamedTemporaryFile
import psutil as ps

#TODO xsorted
class SortError(Exception):
    pass


def parse_memory(s):
    if s[-1].lower() == 'k':
        return int(s[:-1]) * 1024
    elif s[-1].lower() == 'm':
        return int(s[:-1]) * 1024 * 1024
    elif s[-1].lower() == 'g':
        return int(s[:-1]) * 1024 * 1024 * 1024
    else:
        return int(s)

def is_memsort(f):
    vm = ps.virtual_memory()
    available_mem = vm.available
    needs_mem = os.stat(f).st_size * 2.5
    return available_mem > needs_mem

rm = re.compile('[!$ ,:;|\t]+') #TODO
def guess_sep(s):
    try:
        sep = csv.Sniffer().sniff(s).delimiter
        if rm.search(sep):
            return sep
        else:
            return None
    except csv.Error as e:
        if str(e) == 'Could not determine delimiter':
            return None

_validnum = re.compile("\d*\.?\d+")
def _tonum(s):
    s = s.replace("'",'').replace('"','')
    if not s:
        return 0.0
    if _validnum.match(s):
        return float(s)
    else:
        return float("".join(map(lambda x: str(ord(x)), s))) * 10 ** 8

class LargeSort(object):
    def __init__(self, filename,
              header=True, sep=None, 
              key=None, reverse=False,
              max_mem="1M", tmpdir=None):
        self.filename = filename
        self.fp = open(filename)
        self.header = header
        if self.header:
            self.headstr = self.fp.readline()
        else:
            self.headstr = ""
        self.sep = sep or guess_sep(self.headstr)
        if key == "num":
            self.key = self._bynum
        elif key == "lower":
            self.key = self._bylower
        else:
            self.key = key
        self.reverse = reverse
        self.max_mem = parse_memory(max_mem)
        if self.header:
            self.columns = [re.sub(r'(^[\'"]+|[\'"]+$)', '', c) for c in self.headstr.split(self.sep)]
        else:
            self.columns = []
        self.kw = dict(key=self.key, reverse=self.reverse)
        self.tmpdir = tmpdir
        self.split_filenames = []
        self.output = None

    def _bynum(self, line):
        return [_tonum(s) for s in line.split(self.sep)]
    
    def _bylower(self, line):
        return [s for s in line.lower().split(self.sep)]
    
    def tmpsplit(self):
        """Split into smaller files of maximum size and return the filenames.
        """
        while True:
            lines = self.fp.readlines(self.max_mem)
            if lines == []:
                break
            with NamedTemporaryFile(prefix=os.path.basename(sys.argv[0]),
                                    delete=False, mode='w', dir=self.tmpdir) as w:
                self.split_filenames.append(w.name)
                w.writelines(sorted(lines, **self.kw))        
        return map(open, self.split_filenames)
    
    def memorysort(self, outputfilename=None):
        self.output = outputfilename or self.filename + ".sort"
        with open(self.output, "w") as w:
            if self.header:
                w.write(self.headstr)
            w.writelines(sorted(self.fp, **self.kw))
        
        return self.output
    
    def mergesort(self, outputfilename=None):
        self.output = outputfilename or self.filename + ".sort"
        with open(self.output, "w") as w:
            if self.header:
                w.write(self.headstr)
            w.writelines(heapq.merge(*self.tmpsplit(), **self.kw))
        self.clean()
        return self.output
    
    def sort(self, outputfilename=None):
        if is_memsort(self.filename):
            return self.memorysort(outputfilename)
        else:
            return self.mergesort(outputfilename)
    
    def clean(self):
        for f in self.split_filenames:
            os.remove(f)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m',
                        '--mem',
                        help='memory to use for sorting default 100M (ex.100K, 2048K, 10M, 1G)',
                        default='100M')
    parser.add_argument('filename',
                        metavar='<filename>',
                        nargs=1,
                        help='Target File')
    parser.add_argument('-s',
                        '--sep',
                        help='Separated Delimitter Character; if Unknown char then "guess"',
                        default="")
    parser.add_argument('-c',
                        '--columns',
                        help='sorting columns ; comma separated value',
                        default=[])
    parser.add_argument('-o',
                        '--outputfile',
                        help='outputfile path: default is Same Directory write',
                        default=None)
    parser.add_argument('-N',
                        '--noheader',
                        action='store_true',
                        default=False,
                        help='Noheader')
    parser.add_argument('-r',
                        '--reverse',
                        action='store_true',
                        default=False,
                        help='sort reversed')
    parser.add_argument('-n',
                        '--numeric',
                        help='sort by numeric',
                        default=None)
    parser.add_argument('-l',
                        '--lower',
                        help='sort by lower string',
                        default=None)
    args = parser.parse_args()
    if args.columns != [] and args.columns:
        args.columns = args.columns.split(",")

    s = LargeSort(filename=args.filename[0],
                sep=args.sep,
                key=args.numeric or args.lower, #TODO
                header=args.noheader is False,
                reverse=args.reverse,
                max_mem=args.mem)
    s.sort(args.outputfile)


if __name__ == '__main__':
    import argparse
    main()