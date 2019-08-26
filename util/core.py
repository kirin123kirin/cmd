#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
from datetime import datetime

from dateutil.parser._parser import parser, parserinfo


__all__ = [
    "isposix",
    "iswin",
    "getencoding",
    "flatten",
    "binopen",
    "opener",
    "binchunk",
    "to_datetime",
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


ZEN = "".join(chr(0xff01 + i) for i in range(94))
HAN = "".join(chr(0x21 + i) for i in range(94))

def to_hankaku(s):
    return s.translate(str.maketrans(ZEN, HAN))
def to_zenkaku(s):
    return s.translate(str.maketrans(HAN, ZEN))

class lazydate(object):
    class _jpinfo(parserinfo):
        WEEKDAYS = [
                ("Mon", "/曜日", "/曜", "Monday"),
                ("Tue", "火曜日", "火曜", "火", "Tuesday"),
                ("Wed", "水曜日", "水曜", "水", "Wednesday"),
                ("Thu", "木曜日", "木曜", "木", "Thursday"),
                ("Fri", "金曜日", "金曜", "金", "Friday"),
                ("Sat", "土曜日", "土曜", "土", "Saturday"),
                ("Sun", "日曜日", "日曜", "日", "Sunday")]
        HMS = [
                ("h", "時", "hour", "hours"),
                ("m", "分", "minute", "minutes"),
                ("s", "秒", "second", "seconds")]
        AMPM = [
                ("am", "ａｍ", "午前", "a"),
                ("pm", "ｐｍ", "午後", "p")]

    parse = parser(info=_jpinfo()).parse

    d2g = {
        datetime(645,7,17)  : "大化",
        datetime(650,3,22)  : "白雉",
        datetime(686,8,14)  : "朱鳥",
        datetime(701,5,3)   : "大宝",
        datetime(704,6,16)  : "慶雲",
        datetime(708,2,7)   : "和銅",
        datetime(715,10,3)  : "霊亀",
        datetime(717,12,24) : "養老",
        datetime(724,3,3)   : "神亀",
        datetime(729,9,2)   : "天平",
        datetime(749,5,4)   : "天平感宝",
        datetime(749,8,19)  : "天平勝宝",
        datetime(757,9,6)   : "天平宝字",
        datetime(765,2,1)   : "天平神護",
        datetime(767,9,13)  : "神護景雲",
        datetime(770,10,23) : "宝亀",
        datetime(781,1,30)  : "天応",
        datetime(782,9,30)  : "延暦",
        datetime(806,6,8)   : "大同",
        datetime(810,10,20) : "弘仁",
        datetime(824,2,8)   : "天長",
        datetime(834,2,14)  : "承和",
        datetime(848,7,16)  : "嘉祥",
        datetime(851,6,1)   : "仁寿",
        datetime(854,12,23) : "斉衡",
        datetime(857,3,20)  : "天安",
        datetime(859,5,20)  : "貞観",
        datetime(877,6,1)   : "元慶",
        datetime(885,3,11)  : "仁和",
        datetime(889,5,30)  : "寛平",
        datetime(898,5,20)  : "昌泰",
        datetime(901,8,31)  : "延喜",
        datetime(923,5,29)  : "延長",
        datetime(931,5,16)  : "承平",
        datetime(938,6,22)  : "天慶",
        datetime(947,5,15)  : "天暦",
        datetime(957,11,21) : "天徳",
        datetime(961,3,5)   : "応和",
        datetime(964,8,19)  : "康保",
        datetime(968,9,8)   : "安和",
        datetime(970,5,3)   : "天禄",
        datetime(974,1,16)  : "天延",
        datetime(976,8,11)  : "貞元",
        datetime(978,12,31) : "天元",
        datetime(983,5,29)  : "永観",
        datetime(985,5,19)  : "寛和",
        datetime(987,5,5)   : "永延",
        datetime(989,9,10)  : "永祚",
        datetime(990,11,26) : "正暦",
        datetime(995,3,25)  : "長徳",
        datetime(999,2,1)   : "長保",
        datetime(1004,8,8)  : "寛弘",
        datetime(1013,2,8)  : "長和",
        datetime(1017,5,21) : "寛仁",
        datetime(1021,3,17) : "治安",
        datetime(1024,8,19) : "万寿",
        datetime(1028,8,18) : "長元",
        datetime(1037,5,9)  : "長暦",
        datetime(1040,12,16): "長久",
        datetime(1044,12,16): "寛徳",
        datetime(1046,5,22) : "永承",
        datetime(1053,2,2)  : "天喜",
        datetime(1058,9,19) : "康平",
        datetime(1065,9,4)  : "治暦",
        datetime(1069,5,6)  : "延久",
        datetime(1074,9,16) : "承保",
        datetime(1077,12,5) : "承暦",
        datetime(1081,3,22) : "永保",
        datetime(1084,3,15) : "応徳",
        datetime(1087,5,11) : "寛治",
        datetime(1095,1,23) : "嘉保",
        datetime(1097,1,3)  : "永長",
        datetime(1097,12,27): "承徳",
        datetime(1099,9,15) : "康和",
        datetime(1104,3,8)  : "長治",
        datetime(1106,5,13) : "嘉承",
        datetime(1108,9,9)  : "天仁",
        datetime(1110,7,31) : "天永",
        datetime(1113,8,25) : "永久",
        datetime(1118,4,25) : "元永",
        datetime(1120,5,9)  : "保安",
        datetime(1124,5,18) : "天治",
        datetime(1126,2,15) : "大治",
        datetime(1131,2,28) : "天承",
        datetime(1132,9,21) : "長承",
        datetime(1135,6,10) : "保延",
        datetime(1141,8,13) : "永治",
        datetime(1142,5,25) : "康治",
        datetime(1144,3,28) : "天養",
        datetime(1145,8,12) : "久安",
        datetime(1151,2,14) : "仁平",
        datetime(1154,12,4) : "久寿",
        datetime(1156,5,18) : "保元",
        datetime(1159,5,9)  : "平治",
        datetime(1160,2,18) : "永暦",
        datetime(1161,9,24) : "応保",
        datetime(1163,5,4)  : "長寛",
        datetime(1165,7,14) : "永万",
        datetime(1166,9,23) : "仁安",
        datetime(1169,5,6)  : "嘉応",
        datetime(1171,5,27) : "承安",
        datetime(1175,8,16) : "安元",
        datetime(1177,8,29) : "治承",
        datetime(1181,8,25) : "養和",
        datetime(1182,6,29) : "寿永",
        datetime(1184,5,27) : "元暦",
        datetime(1185,9,9)  : "文治",
        datetime(1190,5,16) : "建久",
        datetime(1199,5,23) : "正治",
        datetime(1201,3,19) : "建仁",
        datetime(1204,3,23) : "元久",
        datetime(1206,6,5)  : "建永",
        datetime(1207,11,16): "承元",
        datetime(1211,4,23) : "建暦",
        datetime(1214,1,18) : "建保",
        datetime(1219,5,27) : "承久",
        datetime(1222,5,25) : "貞応",
        datetime(1224,12,31): "元仁",
        datetime(1225,5,28) : "嘉禄",
        datetime(1228,1,18) : "安貞",
        datetime(1229,3,31) : "寛喜",
        datetime(1232,4,23) : "貞永",
        datetime(1233,5,25) : "天福",
        datetime(1234,11,27): "文暦",
        datetime(1235,11,1) : "嘉禎",
        datetime(1238,12,30): "暦仁",
        datetime(1239,3,13) : "延応",
        datetime(1240,8,5)  : "仁治",
        datetime(1243,3,18) : "寛元",
        datetime(1247,4,5)  : "宝治",
        datetime(1249,5,2)  : "建長",
        datetime(1256,10,24): "康元",
        datetime(1257,3,31) : "正嘉",
        datetime(1259,4,20) : "正元",
        datetime(1260,5,24) : "文応",
        datetime(1261,3,22) : "弘長",
        datetime(1264,3,27) : "文永",
        datetime(1275,5,22) : "建治",
        datetime(1278,3,23) : "弘安",
        datetime(1288,5,29) : "正応",
        datetime(1293,9,6)  : "永仁",
        datetime(1299,5,25) : "正安",
        datetime(1302,12,10): "乾元",
        datetime(1303,9,16) : "嘉元",
        datetime(1307,1,18) : "徳治",
        datetime(1308,11,22): "延慶",
        datetime(1311,5,17) : "応長",
        datetime(1312,4,27) : "正和",
        datetime(1317,3,16) : "文保",
        datetime(1319,5,18) : "元応",
        datetime(1321,3,22) : "元亨",
        datetime(1324,12,25): "正中",
        datetime(1326,5,28) : "嘉暦",
        datetime(1329,9,22) : "元徳",
        datetime(1331,9,11) : "元弘",
        datetime(1332,5,23) : "正慶",
        datetime(1334,3,5)  : "建武",
        datetime(1336,4,11) : "延元",
        datetime(1340,5,25) : "興国",
        datetime(1347,1,20) : "正平",
        datetime(1370,8,16) : "建徳",
        datetime(1372,5,1)  : "文中",
        datetime(1375,6,26) : "天授",
        datetime(1381,3,6)  : "弘和",
        datetime(1384,5,18) : "元中",
        datetime(1338,10,11): "暦応",
        datetime(1342,6,1)  : "康永",
        datetime(1345,11,15): "貞和",
        datetime(1350,4,4)  : "観応",
        datetime(1352,11,4) : "文和",
        datetime(1356,4,29) : "延文",
        datetime(1361,5,4)  : "康安",
        datetime(1362,10,11): "貞治",
        datetime(1368,3,7)  : "応安",
        datetime(1375,3,29) : "永和",
        datetime(1379,4,9)  : "康暦",
        datetime(1381,3,20) : "永徳",
        datetime(1384,3,19) : "至徳",
        datetime(1387,10,5) : "嘉慶",
        datetime(1389,3,7)  : "康応",
        datetime(1390,4,12) : "明徳",
        datetime(1394,8,2)  : "応永",
        datetime(1428,6,10) : "正長",
        datetime(1429,10,3) : "永享",
        datetime(1441,3,10) : "嘉吉",
        datetime(1444,2,23) : "文安",
        datetime(1449,8,16) : "宝徳",
        datetime(1452,8,10) : "享徳",
        datetime(1455,9,6)  : "康正",
        datetime(1457,10,16): "長禄",
        datetime(1461,2,1)  : "寛正",
        datetime(1466,3,14) : "文正",
        datetime(1467,4,9)  : "応仁",
        datetime(1469,6,8)  : "文明",
        datetime(1487,8,9)  : "長享",
        datetime(1489,9,16) : "延徳",
        datetime(1492,8,12) : "明応",
        datetime(1501,3,18) : "文亀",
        datetime(1504,3,16) : "永正",
        datetime(1521,9,23) : "大永",
        datetime(1528,9,3)  : "享禄",
        datetime(1532,8,29) : "天文",
        datetime(1555,11,7) : "弘治",
        datetime(1558,3,18) : "永禄",
        datetime(1570,5,27) : "元亀",
        datetime(1573,8,25) : "天正",
        datetime(1593,1,10) : "文禄",
        datetime(1596,12,16): "慶長",
        datetime(1615,9,5)  : "元和",
        datetime(1624,4,17) : "寛永",
        datetime(1645,1,13) : "正保",
        datetime(1648,4,7)  : "慶安",
        datetime(1652,10,20): "承応",
        datetime(1655,5,18) : "明暦",
        datetime(1658,8,21) : "万治",
        datetime(1661,5,23) : "寛文",
        datetime(1673,10,30): "延宝",
        datetime(1681,11,9) : "天和",
        datetime(1684,4,5)  : "貞享",
        datetime(1688,10,23): "元禄",
        datetime(1704,4,16) : "宝永",
        datetime(1711,6,11) : "正徳",
        datetime(1716,8,9)  : "享保",
        datetime(1736,6,7)  : "元文",
        datetime(1741,4,12) : "寛保",
        datetime(1744,4,3)  : "延享",
        datetime(1748,8,5)  : "寛延",
        datetime(1751,12,14): "宝暦",
        datetime(1764,6,30) : "明和",
        datetime(1772,12,10): "安永",
        datetime(1781,4,25) : "天明",
        datetime(1789,2,19) : "寛政",
        datetime(1801,3,19) : "享和",
        datetime(1804,3,22) : "文化",
        datetime(1818,5,26) : "文政",
        datetime(1831,1,23) : "天保",
        datetime(1845,1,9)  : "弘化",
        datetime(1848,4,1)  : "嘉永",
        datetime(1855,1,15) : "安政",
        datetime(1860,4,8)  : "万延",
        datetime(1861,3,29) : "文久",
        datetime(1864,3,27) : "元治",
        datetime(1865,5,1)  : "慶応",
        datetime(1868,10,23): "明治",
        datetime(1912,7,30) : "大正",
        datetime(1926,12,25): "昭和",
        datetime(1989,1,8)  : "平成",
        datetime(2019,5,1)  : "令和",
    }

    didx = list(d2g)
    rsp = re.compile(r"\s\s+")
    isdirty = re.compile(r"[^\d\s/\-:]").search
    is4num = re.compile("\d{4}").match
    
    def __init__(self, timestr):
        if not timestr:
            raise ValueError(timestr)
        ts = __class__.rsp.sub("", to_hankaku(timestr).strip().replace("年", "/").replace("月", "/").replace("_", "-"))
        self.timestr = self.__repairstr(ts) if __class__.isdirty(ts) else ts
        self._dt = None

    def __repairstr(self, timestr):
        for dy, g in __class__.d2g.items():
            i = dy.year - 1
            pattern = r"(?:" + g + r"[\.,\- ]?)((?:[0-9]{1,2}|元))\s?(年?)"
            
            reret = re.search(pattern, timestr)
            if reret:
                n = reret.group(1)
                edit = "{}{}".format(int("1" if n == "元" else n) + i, reret.group(2))
                return timestr.replace(reret.group(0), edit)
        else:
            return timestr

    @property
    def dt(self):
        if self._dt is None:
            self._dt = self.to_datetime()
        return self._dt

    def to_datetime(self, form=None):
        if __class__.is4num(self.timestr):
            ts = str(datetime.now().year) + self.timestr
            try:
                __class__.parse(ts)
            except ValueError:
                pass
            else:
                self.timestr = ts
        
        if form is None:
            return __class__.parse(self.timestr)
        return __class__.parse(self.timestr).strftime(form)

    def to_gengo(self, form="年%m月%d日 %H:%M:%S"):
        item = self.dt
        if os.name == "nt":
            strftime = lambda x: x.strftime(form.encode('unicode-escape').decode()).encode().decode("unicode-escape")
        else:
            strftime = lambda x: x.strftime(form)
            
        try:
            return "{}{}{}".format(__class__.d2g[item], "元", strftime(item))
        except KeyError:
            if type(item) in (str, int):
                item = datetime(item, 1, 1)
            if type(item) == datetime:
                for i, start in enumerate(__class__.d2g, 1):
                    try:
                        end = __class__.didx[i]
                        if start <= item and item < end:
                            keyday = start
                            break
                    except IndexError:
                        keyday = __class__.didx[-1]
                
                gengo =  __class__.d2g[keyday]
                gy = item.year - keyday.year - 1
                if gy < 1:
                    raise ValueError("Unknown Gengo Name. Too old year.", item)
                elif gy == 1:
                    gy = "元"
                return "{}{}{}".format(gengo, gy, strftime(item))

            else:
                raise KeyError(item)

def to_datetime(timestr, form=None):
    return  lazydate(timestr).to_datetime(form)


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


