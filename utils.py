#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__  = 'm.yama'
__license__ = 'MIT'
__date__    = 'Wed Jul 31 17:35:47 2019'
__version__ = '0.0.2'

import os

BUF = 128 * 1024 ** 2

CHUNKSIZE = int(BUF / (64 * 40))

__all__ = [
    'command',
    'which',
    'compressor',
    'decompressor',
    'geturi',
    'csvreader',
    'csvwriter',
    'Counter',
    'duplicates',
    'uniq',
    'timestamp2date',
    'lazydate',
    'to_datetime',
    'to_gengo',
    'is_datetime',
    'finddatetime',
    'hashdigest',
    'hashset',
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
    'create_shortcut',

]


import re
import sys
from datetime import datetime
from itertools import chain, zip_longest
from io import IOBase, StringIO, BytesIO
import fnmatch
from copy import deepcopy
import csv

from pathlib import Path
from collections import namedtuple, _count_elements

import gzip
from subprocess import getstatusoutput
import hashlib
import io
from functools import lru_cache

try:
    import cloudpickle as pickle
except ModuleNotFoundError:
    import pickle

class NotInstalledModuleError(Exception):
    def stderr(self):
        raise __class__("** {} **".format(*self.args)) if self.args else __class__
    def __call__(self, *args, **kw): self.stderr()
    def __getattr__(self, *args, **kw): self.stderr()

try:
    from win32com import client
except ModuleNotFoundError:
    client = NotInstalledModuleError("Please Install command: pip3 install win32com")

from dateutil.parser._parser import parser, parserinfo

from util.core import flatten, binopen, opener, getencoding, isposix, to_hankaku, kanji2int
from util.lex import DATETIME, DATE, ampm, _time

def command(cmd):
    code, dat = getstatusoutput(cmd)
    if code == 0:
        return dat
    else:
        raise RuntimeError(code, dat)

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

class _CsvWriter:
    def __init__(self, fp, mode="w", encoding="cp932", errors="backslashreplace", *args, **kw):
        self._writer = None
        self._entered = False
        self._args = args
        self._kw = kw
        self.fp = opener(fp, mode)

        if hasattr(self.fp, "reconfigure"):
            self.fp.reconfigure(encoding=encoding, errors=errors, newline="")

    @property
    def writer(self):
        if self._writer is None:
            self._writer = csv.writer(self.fp, *self._args, **self._kw)
        return self._writer

    def writerow(self, row):
        self.writer.writerow(row)

    def writerows(self, rows):
        self.writer.writerows(rows)

    # backward compatibility
    write = writerow
    writelines = writerows

    def __enter__(self):
        self._entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._entered:
            self._entered = False
            self.fp.close()
        if exc_type:
            return

    def __del__(self):
        if self._entered:
            self.fp.close()

def csvwriter(rows, fp, mode="w", encoding="cp932", *args, **kw):
    cf = _CsvWriter(fp, mode=mode, encoding=encoding, *args, **kw)
    cf.writerows(rows)
    return cf

def Counter(iterable):
    d = {}
    _count_elements(d, iterable)
    return d

def duplicates(iterable, key=None, uniq_return=True, callback=lambda x: x):
    d = {}
    get = d.get
    if key is None:
        for e in iterable:
            ke = tuple(e)
            d[ke] = get(ke, []) + [e]
    else:
        for e in iterable:
            ke = tuple(key(e))
            d[ke] = get(ke, []) + [e]

    if uniq_return:
        r = []
        radd = r.append
        for v in d.values():
            if len(v) > 1:
                for _w in v:
                    w = callback(_w)
                    if w in r:
                        continue
                    radd(w)
        return r
    else:
        return sum((callback(v) for v in d.values() if len(v) > 1), [])


def uniq(iterable, key=None, callback=None):
    l = []
    if callback:
        def ladd(x):
            l.append(callback(x))
    else:
        ladd = l.append

    if key is None:
        for i in iterable:
            if i in l:
                continue
            ladd(i)
    else:
        kl = []
        kadd = kl.append

        for i in iterable:
            k = key(i)
            if k in kl:
                continue
            kadd(k)
            ladd(i)
        del kl
    return l


def timestamp2date(x, dfm = "%Y/%m/%d %H:%M"):
    return datetime.fromtimestamp(x).strftime(dfm)

class lazydate(object):
    class jpinfo(parserinfo):
        JUMP = [" ", "　",".", ",", ";", "-", "/", "'",
                "at", "on", "and", "ad", "m", "t", "of",
                "st", "nd", "rd", "th",
                "年", "月", "日",
                ]
        HMS = [("h", "hour", "hours", "時"),
               ("m", "minute", "minutes", "分"),
               ("s", "second", "seconds", "秒")]
        AMPM = [("am", "a", "午前"),
                ("pm", "p", "午後")]

    g2d = {
        '令和': datetime(2019, 5, 1),
        '平成': datetime(1989, 1, 8),
        '昭和': datetime(1926, 12, 25),
        '大正': datetime(1912, 7, 30),
        '明治': datetime(1868, 10, 23),
        '慶応': datetime(1865, 5, 1), '元治': datetime(1864, 3, 27), '文久': datetime(1861, 3, 29), '万延': datetime(1860, 4, 8), '安政': datetime(1855, 1, 15), '嘉永': datetime(1848, 4, 1), '弘化': datetime(1845, 1, 9), '天保': datetime(1831, 1, 23), '文政': datetime(1818, 5, 26), '文化': datetime(1804, 3, 22), '享和': datetime(1801, 3, 19), '寛政': datetime(1789, 2, 19), '天明': datetime(1781, 4, 25), '安永': datetime(1772, 12, 10), '明和': datetime(1764, 6, 30), '宝暦': datetime(1751, 12, 14), '寛延': datetime(1748, 8, 5), '延享': datetime(1744, 4, 3), '寛保': datetime(1741, 4, 12), '元文': datetime(1736, 6, 7), '享保': datetime(1716, 8, 9), '正徳': datetime(1711, 6, 11), '宝永': datetime(1704, 4, 16), '元禄': datetime(1688, 10, 23), '貞享': datetime(1684, 4, 5), '天和': datetime(1681, 11, 9), '延宝': datetime(1673, 10, 30), '寛文': datetime(1661, 5, 23), '万治': datetime(1658, 8, 21), '明暦': datetime(1655, 5, 18), '承応': datetime(1652, 10, 20), '慶安': datetime(1648, 4, 7), '正保': datetime(1645, 1, 13), '寛永': datetime(1624, 4, 17), '元和': datetime(1615, 9, 5), '慶長': datetime(1596, 12, 16), '文禄': datetime(1593, 1, 10), '天正': datetime(1573, 8, 25), '元亀': datetime(1570, 5, 27), '永禄': datetime(1558, 3, 18), '弘治': datetime(1555, 11, 7), '天文': datetime(1532, 8, 29), '享禄': datetime(1528, 9, 3), '大永': datetime(1521, 9, 23), '永正': datetime(1504, 3, 16), '文亀': datetime(1501, 3, 18), '明応': datetime(1492, 8, 12), '延徳': datetime(1489, 9, 16), '長享': datetime(1487, 8, 9), '文明': datetime(1469, 6, 8), '応仁': datetime(1467, 4, 9), '文正': datetime(1466, 3, 14),
        '寛正': datetime(1461, 2, 1), '長禄': datetime(1457, 10, 16), '康正': datetime(1455, 9, 6), '享徳': datetime(1452, 8, 10), '宝徳': datetime(1449, 8, 16), '文安': datetime(1444, 2, 23), '嘉吉': datetime(1441, 3, 10), '永享': datetime(1429, 10, 3), '正長': datetime(1428, 6, 10), '応永': datetime(1394, 8, 2), '明徳': datetime(1390, 4, 12), '康応': datetime(1389, 3, 7), '嘉慶': datetime(1387, 10, 5), '至徳': datetime(1384, 3, 19), '永徳': datetime(1381, 3, 20), '康暦': datetime(1379, 4, 9), '永和': datetime(1375, 3, 29), '応安': datetime(1368, 3, 7), '貞治': datetime(1362, 10, 11), '康安': datetime(1361, 5, 4), '延文': datetime(1356, 4, 29), '文和': datetime(1352, 11, 4), '観応': datetime(1350, 4, 4), '貞和': datetime(1345, 11, 15), '康永': datetime(1342, 6, 1), '暦応': datetime(1338, 10, 11), '元中': datetime(1384, 5, 18), '弘和': datetime(1381, 3, 6), '天授': datetime(1375, 6, 26), '文中': datetime(1372, 5, 1), '建徳': datetime(1370, 8, 16), '正平': datetime(1347, 1, 20), '興国': datetime(1340, 5, 25), '延元': datetime(1336, 4, 11), '建武': datetime(1334, 3, 5), '正慶': datetime(1332, 5, 23), '元弘': datetime(1331, 9, 11), '元徳': datetime(1329, 9, 22), '嘉暦': datetime(1326, 5, 28), '正中': datetime(1324, 12, 25), '元亨': datetime(1321, 3, 22), '元応': datetime(1319, 5, 18), '文保': datetime(1317, 3, 16), '正和': datetime(1312, 4, 27), '応長': datetime(1311, 5, 17), '延慶': datetime(1308, 11, 22), '徳治': datetime(1307, 1, 18), '嘉元': datetime(1303, 9, 16), '乾元': datetime(1302, 12, 10), '正安': datetime(1299, 5, 25), '永仁': datetime(1293, 9, 6), '正応': datetime(1288, 5, 29), '弘安': datetime(1278, 3, 23), '建治': datetime(1275, 5, 22), '文永': datetime(1264, 3, 27), '弘長': datetime(1261, 3, 22), '文応': datetime(1260, 5, 24), '正元': datetime(1259, 4, 20), '正嘉': datetime(1257, 3, 31), '康元': datetime(1256, 10, 24), '建長': datetime(1249, 5, 2), '宝治': datetime(1247, 4, 5), '寛元': datetime(1243, 3, 18), '仁治': datetime(1240, 8, 5), '延応': datetime(1239, 3, 13), '暦仁': datetime(1238, 12, 30), '嘉禎': datetime(1235, 11, 1), '文暦': datetime(1234, 11, 27), '天福': datetime(1233, 5, 25), '貞永': datetime(1232, 4, 23), '寛喜': datetime(1229, 3, 31), '安貞': datetime(1228, 1, 18), '嘉禄': datetime(1225, 5, 28), '元仁': datetime(1224, 12, 31), '貞応': datetime(1222, 5, 25), '承久': datetime(1219, 5, 27), '建保': datetime(1214, 1, 18), '建暦': datetime(1211, 4, 23), '承元': datetime(1207, 11, 16), '建永': datetime(1206, 6, 5), '元久': datetime(1204, 3, 23), '建仁': datetime(1201, 3, 19), '正治': datetime(1199, 5, 23), '建久': datetime(1190, 5, 16), '文治': datetime(1185, 9, 9), '元暦': datetime(1184, 5, 27), '寿永': datetime(1182, 6, 29), '養和': datetime(1181, 8, 25), '治承': datetime(1177, 8, 29), '安元': datetime(1175, 8, 16), '承安': datetime(1171, 5, 27), '嘉応': datetime(1169, 5, 6), '仁安': datetime(1166, 9, 23), '永万': datetime(1165, 7, 14), '長寛': datetime(1163, 5, 4), '応保': datetime(1161, 9, 24), '永暦': datetime(1160, 2, 18),
        '平治': datetime(1159, 5, 9), '保元': datetime(1156, 5, 18), '久寿': datetime(1154, 12, 4), '仁平': datetime(1151, 2, 14), '久安': datetime(1145, 8, 12), '天養': datetime(1144, 3, 28), '康治': datetime(1142, 5, 25), '永治': datetime(1141, 8, 13), '保延': datetime(1135, 6, 10), '長承': datetime(1132, 9, 21), '天承': datetime(1131, 2, 28), '大治': datetime(1126, 2, 15), '天治': datetime(1124, 5, 18), '保安': datetime(1120, 5, 9), '元永': datetime(1118, 4, 25), '永久': datetime(1113, 8, 25), '天永': datetime(1110, 7, 31), '天仁': datetime(1108, 9, 9), '嘉承': datetime(1106, 5, 13), '長治': datetime(1104, 3, 8), '康和': datetime(1099, 9, 15), '承徳': datetime(1097, 12, 27), '永長': datetime(1097, 1, 3), '嘉保': datetime(1095, 1, 23), '寛治': datetime(1087, 5, 11), '応徳': datetime(1084, 3, 15), '永保': datetime(1081, 3, 22), '承暦': datetime(1077, 12, 5), '承保': datetime(1074, 9, 16), '延久': datetime(1069, 5, 6), '治暦': datetime(1065, 9, 4), '康平': datetime(1058, 9, 19), '天喜': datetime(1053, 2, 2), '永承': datetime(1046, 5, 22), '寛徳': datetime(1044, 12, 16), '長久': datetime(1040, 12, 16), '長暦': datetime(1037, 5, 9), '長元': datetime(1028, 8, 18), '万寿': datetime(1024, 8, 19), '治安': datetime(1021, 3, 17), '寛仁': datetime(1017, 5, 21), '長和': datetime(1013, 2, 8), '寛弘': datetime(1004, 8, 8), '長保': datetime(999, 2, 1), '長徳': datetime(995, 3, 25), '正暦': datetime(990, 11, 26), '永祚': datetime(989, 9, 10), '永延': datetime(987, 5, 5), '寛和': datetime(985, 5, 19), '永観': datetime(983, 5, 29), '天元': datetime(978, 12, 31), '貞元': datetime(976, 8, 11), '天延': datetime(974, 1, 16), '天禄': datetime(970, 5, 3), '安和': datetime(968, 9, 8), '康保': datetime(964, 8, 19), '応和': datetime(961, 3, 5), '天徳': datetime(957, 11, 21), '天暦': datetime(947, 5, 15), '天慶': datetime(938, 6, 22), '承平': datetime(931, 5, 16), '延長': datetime(923, 5, 29), '延喜': datetime(901, 8, 31), '昌泰': datetime(898, 5, 20), '寛平': datetime(889, 5, 30), '仁和': datetime(885, 3, 11), '元慶': datetime(877, 6, 1), '貞観': datetime(859, 5, 20), '天安': datetime(857, 3, 20), '斉衡': datetime(854, 12, 23), '仁寿': datetime(851, 6, 1), '嘉祥': datetime(848, 7, 16), '承和': datetime(834, 2, 14), '天長': datetime(824, 2, 8), '弘仁': datetime(810, 10, 20), '大同': datetime(806, 6, 8), '延暦': datetime(782, 9, 30), '天応': datetime(781, 1, 30), '宝亀': datetime(770, 10, 23), '神護景雲': datetime(767, 9, 13), '天平神護': datetime(765, 2, 1), '天平宝字': datetime(757, 9, 6), '天平勝宝': datetime(749, 8, 19), '天平感宝': datetime(749, 5, 4), '天平': datetime(729, 9, 2), '神亀': datetime(724, 3, 3), '養老': datetime(717, 12, 24), '霊亀': datetime(715, 10, 3), '和銅': datetime(708, 2, 7), '慶雲': datetime(704, 6, 16), '大宝': datetime(701, 5, 3), '朱鳥': datetime(686, 8, 14), '白雉': datetime(650, 3, 22),
        '大化': datetime(645, 7, 17)
    }
    
    def __init__(self, timestr, parserinfo=None, **kwargs):
        """
    
        Parse a string in one of the supported formats, using the
        ``parserinfo`` parameters.
        
            default object -> lazydate.jpinfo
    
        :param timestr:
            A string containing a date/time stamp.
    
        :param parserinfo:
            A :class:`parserinfo` object containing parameters for the parser.
            If ``None``, the default arguments to the :class:`parserinfo`
            constructor are used.
    
        The ``**kwargs`` parameter takes the following keyword arguments:
    
        :param default:
            The default datetime object, if this is a datetime object and not
            ``None``, elements specified in ``timestr`` replace elements in the
            default object.
    
        :param ignoretz:
            If set ``True``, time zones in parsed strings are ignored and a naive
            :class:`datetime` object is returned.
    
        :param tzinfos:
            Additional time zone names / aliases which may be present in the
            string. This argument maps time zone names (and optionally offsets
            from those time zones) to time zones. This parameter can be a
            dictionary with timezone aliases mapping time zone names to time
            zones or a function taking two parameters (``tzname`` and
            ``tzoffset``) and returning a time zone.
    
            The timezones to which the names are mapped can be an integer
            offset from UTC in seconds or a :class:`tzinfo` object.
    
            .. doctest::
               :options: +NORMALIZE_WHITESPACE
    
                >>> from dateutil.parser import parse
                >>> from dateutil.tz import gettz
                >>> tzinfos = {"BRST": -7200, "CST": gettz("America/Chicago")}
                >>> parse("2012-01-19 17:21:00 BRST", tzinfos=tzinfos)
                datetime(2012, 1, 19, 17, 21, tzinfo=tzoffset(u'BRST', -7200))
                >>> parse("2012-01-19 17:21:00 CST", tzinfos=tzinfos)
                datetime(2012, 1, 19, 17, 21,
                                  tzinfo=tzfile('/usr/share/zoneinfo/America/Chicago'))
    
            This parameter is ignored if ``ignoretz`` is set.
    
        :param dayfirst:
            Whether to interpret the first value in an ambiguous 3-integer date
            (e.g. 01/05/09) as the day (``True``) or month (``False``). If
            ``yearfirst`` is set to ``True``, this distinguishes between YDM and
            YMD. If set to ``None``, this value is retrieved from the current
            :class:`parserinfo` object (which itself defaults to ``False``).
    
        :param yearfirst:
            Whether to interpret the first value in an ambiguous 3-integer date
            (e.g. 01/05/09) as the year. If ``True``, the first number is taken to
            be the year, otherwise the last number is taken to be the year. If
            this is set to ``None``, the value is retrieved from the current
            :class:`parserinfo` object (which itself defaults to ``False``).
    
        :param fuzzy:
            default True.
            Whether to allow fuzzy parsing, allowing for string like "Today is
            January 1, 2047 at 8:21:00AM".
    
        :param fuzzy_with_tokens:
            If ``True``, ``fuzzy`` is automatically set to True, and the parser
            will return a tuple where the first element is the parsed
            :class:`datetime` datetimestamp and the second element is
            a tuple containing the portions of the string which were ignored:
    
            .. doctest::
    
                >>> from dateutil.parser import parse
                >>> parse("Today is January 1, 2047 at 8:21:00AM", fuzzy_with_tokens=True)
                (datetime(2047, 1, 1, 8, 21), (u'Today is ', u' ', u'at '))
    
        :return:
            Returns a :class:`datetime` object or, if the
            ``fuzzy_with_tokens`` option is ``True``, returns a tuple, the
            first element being a :class:`datetime` object, the second
            a tuple containing the fuzzy tokens.
    
        :raises ValueError:
            Raised for invalid or unknown string format, if the provided
            :class:`tzinfo` is not in a valid format, or if an invalid date
            would be created.
    
        :raises OverflowError:
            Raised if the parsed date exceeds the largest valid C integer on
            your system.
        """
        self._dt = None
        self.timestr = timestr
        self.parserinfo = parserinfo or __class__.jpinfo()
        self._kwargs = kwargs
        
        if isinstance(timestr, datetime):
            self._dt = timestr
            self.timestr = self.repairstr = str(timestr)
        
        if not isinstance(timestr, str):
            self.timestr = str(timestr)
            
        if "fuzzy" not in kwargs:
            self._kwargs["fuzzy"] = True
    
    @classmethod
    @lru_cache(8)
    def gengo2date(cls, timestr):
        g = next((d for d in cls.g2d if d in timestr), None)
        if g is None:
            return timestr
    
        dy = cls.g2d[g]
        i = dy.year - 1
        pattern = r"(?:" + g + r"[\.,\- ]?)((?:[0-9]{1,2}|元))\s?(年?)"
    
        reret = re.search(pattern, timestr)
        if reret:
            n = reret.group(1)
            edit = "{}{}".format(int("1" if n == "元" else n) + i, reret.group(2))
            return timestr.replace(reret.group(0), edit)
    
        return timestr
    
    @staticmethod
    def repair_ampm(s,
        _ng_ampm = re.compile('(\\s*(?:[AaPp]\\.?[Mm]\\.?|午[前後]))((?:\\s*(?:1[0-9]|2[0-4]||0?[0-9])\\s*?[\\.:時]?\\s*(?:[1-5][0-9]|0?[0-9])\\s*?[\\.:分]?\\s*(?:[1-5][0-9]|0?[0-9])\\s*?(?:秒|[Ss]ec(?:onds)?)??\\s*(?:[,\\.]?\\d+)?(?:\\s*(?:[+\\-]\\d{4})\\s*\\(?(?:[ABCDEFGHIJKLMNOPRSTUVWY][ABCDEFGHIJKLMNOPRSTUVWXYZ][ABCDGHKLMNORSTUVWZ][1DST][T])\\)?|\\s*(?:[+\\-]\\d{4})|\\s*\\(?(?:[ABCDEFGHIJKLMNOPRSTUVWY][ABCDEFGHIJKLMNOPRSTUVWXYZ][ABCDGHKLMNORSTUVWZ][1DST][T])\\)?)?)[^\\s]*)')
        ):
        return _ng_ampm.sub(" \\2 \\1", s).replace("  ", " ")
    
    def parse(self, timestr=None, **kw):
        """
        :param timestr:
            A string containing a date/time stamp.
    
        The ``**kw`` parameter takes the following keyword arguments:
    
        :param default:
            The default datetime object, if this is a datetime object and not
            ``None``, elements specified in ``timestr`` replace elements in the
            default object.
    
        :param ignoretz:
            If set ``True``, time zones in parsed strings are ignored and a naive
            :class:`datetime` object is returned.
    
        :param tzinfos:
            Additional time zone names / aliases which may be present in the
            string. This argument maps time zone names (and optionally offsets
            from those time zones) to time zones. This parameter can be a
            dictionary with timezone aliases mapping time zone names to time
            zones or a function taking two parameters (``tzname`` and
            ``tzoffset``) and returning a time zone.
    
            The timezones to which the names are mapped can be an integer
            offset from UTC in seconds or a :class:`tzinfo` object.
    
            .. doctest::
               :options: +NORMALIZE_WHITESPACE
    
                >>> from dateutil.parser import parse
                >>> from dateutil.tz import gettz
                >>> tzinfos = {"BRST": -7200, "CST": gettz("America/Chicago")}
                >>> parse("2012-01-19 17:21:00 BRST", tzinfos=tzinfos)
                datetime(2012, 1, 19, 17, 21, tzinfo=tzoffset(u'BRST', -7200))
                >>> parse("2012-01-19 17:21:00 CST", tzinfos=tzinfos)
                datetime(2012, 1, 19, 17, 21,
                                  tzinfo=tzfile('/usr/share/zoneinfo/America/Chicago'))
    
            This parameter is ignored if ``ignoretz`` is set.
    
        :param dayfirst:
            Whether to interpret the first value in an ambiguous 3-integer date
            (e.g. 01/05/09) as the day (``True``) or month (``False``). If
            ``yearfirst`` is set to ``True``, this distinguishes between YDM and
            YMD. If set to ``None``, this value is retrieved from the current
            :class:`parserinfo` object (which itself defaults to ``False``).
    
        :param yearfirst:
            Whether to interpret the first value in an ambiguous 3-integer date
            (e.g. 01/05/09) as the year. If ``True``, the first number is taken to
            be the year, otherwise the last number is taken to be the year. If
            this is set to ``None``, the value is retrieved from the current
            :class:`parserinfo` object (which itself defaults to ``False``).
    
        :param fuzzy:
            default True.
            Whether to allow fuzzy parsing, allowing for string like "Today is
            January 1, 2047 at 8:21:00AM".
    
        :param fuzzy_with_tokens:
            If ``True``, ``fuzzy`` is automatically set to True, and the parser
            will return a tuple where the first element is the parsed
            :class:`datetime` datetimestamp and the second element is
            a tuple containing the portions of the string which were ignored:
    
            .. doctest::
    
                >>> from dateutil.parser import parse
                >>> parse("Today is January 1, 2047 at 8:21:00AM", fuzzy_with_tokens=True)
                (datetime(2047, 1, 1, 8, 21), (u'Today is ', u' ', u'at '))
    
        :return:
            Returns a :class:`datetime` object or, if the
            ``fuzzy_with_tokens`` option is ``True``, returns a tuple, the
            first element being a :class:`datetime` object, the second
            a tuple containing the fuzzy tokens.
    
        :raises ValueError:
            Raised for invalid or unknown string format, if the provided
            :class:`tzinfo` is not in a valid format, or if an invalid date
            would be created.
    
        :raises OverflowError:
            Raised if the parsed date exceeds the largest valid C integer on
            your system.
        """
        if timestr is None and self._dt is not None:
            return self._dt
        if isinstance(timestr, datetime):
            return timestr
        
        repairstr = to_hankaku(timestr or self.timestr)
        repairstr = kanji2int(repairstr)
        repairstr = __class__.gengo2date(repairstr)
        repairstr = __class__.repair_ampm(repairstr)
        
        return parser(self.parserinfo).parse(timestr or repairstr, **{**self._kwargs, **kw})
    
    @property
    def dt(self):
        if self._dt is None:
            self._dt = self.parse()
        return self._dt
    
    def to_datetime(self, timestr=None, form=None):
        if form is None:
            return self.parse(timestr, fuzzy_with_tokens=False)
        if "%ggg" in form:
            return self.to_gengo(timestr, form)
        return self.parse(timestr, fuzzy_with_tokens=False).strftime(form)
    
    
    def to_gengo(self, timestr, form="%ggg年%m月%d日 %H:%M:%S"):
        """
        和暦変換関数
    
        Parameters
        ----------
        timestr : TYPE
            DESCRIPTION.
        form : TYPE, optional
            DESCRIPTION. The default is "%ggg年%m月%d日 %H:%M:%S".
    
        Returns
        -------
        TYPE
            DESCRIPTION.
    
        """
        dt = self.parse(timestr, fuzzy_with_tokens=False)
        g2d = __class__.g2d
    
        gname, gyear = next((g, dt.year - g2d[g].year + 1) for g in g2d if g2d[g] <= dt)
        gengo = "{}{}".format(gname, "元" if gyear == 1 else gyear)
        form = form.replace("%ggg", gengo)
        return dt.strftime(form.encode('unicode-escape').decode()).encode().decode("unicode-escape")


def to_datetime(timestr, form=None):
    if isinstance(timestr, datetime):
        return timestr
    return  lazydate(timestr).to_datetime(form)

def to_gengo(timestr, form=None):
    return  lazydate(str(timestr)).to_gengo(form)

re_datetime = re.compile(DATETIME)
re_date = re.compile(DATE)

def is_datetime(text):
    return re_datetime.search(text) is not None

def is_date(text):
    return re_date.search(text) is not None

def finddatetime(text, callback=to_datetime):
    if is_datetime(text):
        return [callback(x) for x in re_datetime.findall(text) if x.strip()]
    if is_date(text):
        return [callback(x) for x in re_date.findall(text) if x.strip()]
    return []

def hashdigest(x, argo=hashlib.md5):
    return argo(repr(x).encode()).digest()

class hashset(set):
    def __init__(self, bucketfile=None):
        self.bucketfile = bucketfile
        if bucketfile:
            self.bucketfile = os.path.abspath(bucketfile)
            if not os.path.exists(os.path.dirname(self.bucketfile)):
                raise FileNotFoundError("Not Found Directory")
            elif os.path.exists(self.bucketfile):
                with open(self.bucketfile, "rb") as f:
                    super().__init__(pickle.load(f))
                    return

        super().__init__()

    def __contains__(self, val):
        return super().__contains__(hashdigest(val))

    def add(self, val):
        return super().add(hashdigest(val))

    def pop(self, val):
        return super().pop(hashdigest(val))

    def remove(self, val):
        return super().remove(hashdigest(val))

    def update(self, row):
        return super().update(map(hashdigest, row))

    def __del__(self):
        if self and self.bucketfile:
            with io.open(self.bucketfile, "wb") as w:
                pickle.dump(set(self), w)

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

def create_shortcut(inpath, scFileName, outpath=None, icon=None):
    shell   = client.Dispatch('WScript.shell')
    if not outpath:
        desktop = shell.SpecialFolders('Desktop')
        outpath = os.path.join(desktop, scFileName+".lnk")

    if not outpath.lower().endswith(".lnk"):
        outpath += ".lnk"

    shCut                  = shell.CreateShortcut(outpath)
    shCut.TargetPath       = inpath
    shCut.WindowStyle      = 1
    shCut.IconLocation     = icon or inpath
    shCut.WorkingDirectory = os.path.dirname(inpath)

    shCut.Save()


if __name__ == "__main__":

    def test():
        from pathlib import Path
        from util.core import tdir, TMPDIR

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
            assert(isdataframe(pd.Series([],dtype=pd.StringDtype())) is False)

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

        def test_to_datetime():
            a1 = datetime(2001,8,24)
            a2 = datetime(2001,8,24,20,10)
            a3 = datetime(2019,8,24,20,10)

            #日本
            assert(to_datetime("2001/08/24") == a1)
            assert(to_datetime("平成13年08月24日") == a1)
            assert(to_datetime("2001/08/24 20:10") == a2)
            assert(to_datetime("2001年8月24日金曜日 20時10分") == a2)
            assert(to_datetime("2001年8月24日(金) 20時10分") == a2)
            assert(to_datetime("令和元年08月24日PM 08時10分") == a3)
            assert(to_datetime("令和元年08月24日　PM08時10分") == a3)
            assert(to_datetime("令和元年08月24日　午後8時10分") == a3)
            assert(to_datetime("令和元年08/24午後08:10") == a3)

            # #米国 mm-dd-yy
            assert(to_datetime("08-24-01") == a1)
            assert(to_datetime("Friday, August 24th, 2001") == a1)
            assert(to_datetime("Fri Aug. 24, 2001 8:10 p.m.") == a2)
            assert(to_datetime("Fri Aug. 24, 2001 20:10") == a2)

        def test_to_gengo():
            a1 = datetime(1945,8,15)

            assert(to_gengo("昭和２０年８月１５日") == "昭和20年08月15日 00:00:00")
            assert(to_gengo("昭和二十年八月十五日") == "昭和20年08月15日 00:00:00")

        t0 = datetime.now()
        for x, func in list(locals().items()):
            if x.startswith("test_") and callable(func):
                t1 = datetime.now()
                func()
                t2 = datetime.now()
                print("{} : time {}".format(x, t2-t1))
        t3 = datetime.now()
        print("{} : time {}".format(x, t3-t0))
    test()

