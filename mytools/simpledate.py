#!/usr/bin/env python
## -*- coding: utf-8 -*-
# Require Module dateutil (http://labix.org/python-dateutil)

from datetime import datetime
from time import mktime, struct_time
import json
from os.path import dirname

# Dependencies Require Modules
from dateutil.parser import parse as dateutilparse
from dateutil.tz import tzlocal, gettz, tzoffset

__author__  = "m.yama"
__license__ = 'MIT'
__version__ = "0.1.4"
__all__ = ['from_epoch', 'from_ts', 'to_ctime', 'to_dt', 'to_epoch',
           'to_htime', 'to_iso', 'to_isosec', 'to_simple', 'to_ts',
           'tzdic', 'tzregdic']

tzdic = json.load(open(dirname(__file__) + "/dat/tzdict.json")) ## Refferences by geonames.org

tzregdic = { ## RefferenceDocument is 
             ## "http://www.timeanddate.com/library/abbreviations/timezones" at 2013-05-08T12:30+0900
        "A":1,
        "ACDT":10.5,
        "ACST":9.5,
        "ADT":-3,
        "AEDT":11,
        "AEST":10,
        "AFT":4.5,
        "AKDT":-8,
        "AKST":-9,
        "ALMT":6,
        "AMST":-3,
        "AMST":5,
        "AMT":-4,
        "AMT":4,
        "ANAST":12,
        "ANAT":12,
        "AQTT":5,
        "ART":-3,
        "AST":-4,
        "AST":3,
        "AWDT":9,
        "AWST":8,
        "AZOST":0,
        "AZOT":-1,
        "AZST":5,
        "AZT":4,
        "B":2,
        "BNT":8,
        "BOT":-4,
        "BRST":-2,
        "BRT":-3,
        "BST":1,
        "BST":6,
        "BTT":6,
        "C":3,
        "CAST":8,
        "CAT":2,
        "CCT":6.5,
        "CDT":-4,
        "CDT":-5,
        "CEST":2,
        "CET":1,
        "CHADT":13.75,
        "CHAST":12.75,
        "CKT":-10,
        "CLST":-3,
        "CLT":-4,
        "COT":-5,
        "CST":-5,
        "CST":-6,
        "CST":8,
        "CVT":-1,
        "CXT":7,
        "ChST":10,
        "D":4,
        "DAVT":7,
        "E":5,
        "EASST":-5,
        "EAST":-6,
        "EAT":3,
        "ECT":-5,
        "EDT":-4,
        "EDT":11,
        "EEST":3,
        "EET":2,
        "EGST":0,
        "EGT":-1,
        "EST":-5,
        "ET":-5,
        "F":6,
        "FJST":13,
        "FJT":12,
        "FKST":-3,
        "FKT":-4,
        "FNT":-2,
        "G":7,
        "GALT":-6,
        "GAMT":-9,
        "GET":4,
        "GFT":-3,
        "GILT":12,
        "GMT":0,
        "GST":4,
        "GYT":-4,
        "H":8,
        "HAA":-3,
        "HAC":-5,
        "HADT":-9,
        "HAE":-4,
        "HAP":-7,
        "HAR":-6,
        "HAST":-10,
        "HAT":-2.5,
        "HAY":-8,
        "HKT":8,
        "HLV":-4.5,
        "HNA":-4,
        "HNC":-6,
        "HNE":-5,
        "HNP":-8,
        "HNR":-7,
        "HNT":-3.5,
        "HNY":-9,
        "HOVT":7,
        "I":9,
        "ICT":7,
        "IDT":3,
        "IOT":6,
        "IRDT":4.5,
        "IRKST":9,
        "IRKT":9,
        "IRST":3.5,
        "IST":1,
        "IST":2,
        "IST":5.5,
        "JST":9,
        "K":10,
        "KGT":6,
        "KRAST":8,
        "KRAT":8,
        "KST":9,
        "KUYT":4,
        "L":11,
        "LHDT":11,
        "LHST":10.5,
        "LINT":14,
        "M":12,
        "MAGST":12,
        "MAGT":12,
        "MART":-9.5,
        "MAWT":5,
        "MDT":-6,
        "MESZ":2,
        "MEZ":1,
        "MHT":12,
        "MMT":6.5,
        "MSD":4,
        "MSK":4,
        "MST":-7,
        "MUT":4,
        "MVT":5,
        "MYT":8,
        "N":-1,
        "NCT":11,
        "NDT":-2.5,
        "NFT":11.5,
        "NOVST":7,
        "NOVT":6,
        "NPT":5.75,
        "NST":-3.5,
        "NUT":-11,
        "NZDT":13,
        "NZST":12,
        "O":-2,
        "OMSST":7,
        "OMST":7,
        "P":-3,
        "PDT":-7,
        "PET":-5,
        "PETST":12,
        "PETT":12,
        "PGT":10,
        "PHOT":13,
        "PHT":8,
        "PKT":5,
        "PMDT":-2,
        "PMST":-3,
        "PONT":11,
        "PST":-8,
        "PT":-8,
        "PWT":9,
        "PYST":-3,
        "PYT":-4,
        "Q":-4,
        "R":-5,
        "RET":4,
        "S":-6,
        "SAMT":4,
        "SAST":2,
        "SBT":11,
        "SCT":4,
        "SGT":8,
        "SRT":-3,
        "SST":-11,
        "T":-7,
        "TAHT":-10,
        "TFT":5,
        "TJT":5,
        "TKT":13,
        "TLT":9,
        "TMT":5,
        "TVT":12,
        "U":-8,
        "ULAT":8,
        "UTC":0,
        "UYST":-2,
        "UYT":-3,
        "UZT":5,
        "V":-9,
        "VET":-4.5,
        "VLAST":11,
        "VLAT":11,
        "VUT":11,
        "W":-10,
        "WAST":2,
        "WAT":1,
        "WEST":1,
        "WESZ":1,
        "WET":0,
        "WEZ":0,
        "WFT":12,
        "WGST":-2,
        "WGT":-3,
        "WIB":7,
        "WIT":9,
        "WITA":8,
        "WST":1,
        "WST":13,
        "WT":0,
        "X":-11,
        "Y":-12,
        "YAKST":10,
        "YAKT":10,
        "YAPT":10,
        "YEKST":6,
        "YEKT":6,
        "Z":00};

## Return exactry Timezone from User Inputed like Timezone Name.
def _gettzinfo(name):
    if name.lower() == "local":
        return tzlocal()
    else:
        if name.lower() in tzdic:
            exactname = tzdic[name.lower()]
            if isinstance(exactname, list) and len(exactname) > 1:
                raise AttributeError("Timezone Name is not Collect. Please Select %s" % ", ".join(exactname))
            return gettz(exactname)
        elif name.upper() in tzregdic:
            utcdsthour = tzregdic[name.upper()]
            return tzoffset(name.upper(), utcdsthour * 3600)
        else:
            raise ValueError("Unknown Timezone Name :%s ." % name)

def to_dt(s, inptz="local", adjusttz="local"):
    """ When dateutil module can guesses parse
        return datetime object from string or unicode, datetime
        Attributes:
            s : Date like Strings or Unicode String or time_structure or datetimeobject
            
            inptz,adjusttz : TimeZoneName by dateutil.tz.gettz function , and 
                                Extended Usable name bellow.
                                "local" or "UTC" or CityName(ex.tokyo) or 
                                CountryCode(ex.JP *When Only OnePattern Timezone*)
                                TimeZoneRegionCode(ex. JST, ADT)

            return datetime object with tzinfo by adjusttz setting
    """
    if isinstance(s, datetime):
        pass
    elif isinstance(s, struct_time):
        s = datetime(*s[:6])
    elif isinstance(s, str):
        s = dateutilparse(s)
    else:
        raise ValueError("Unknown Date Format Parse %s " % type(s))

    if s.tzinfo is None:
        return s.replace(tzinfo=_gettzinfo(inptz)).astimezone(_gettzinfo(adjusttz))
    else:
        return s.astimezone(_gettzinfo(adjusttz))

def from_epoch(epoch, inptz="UTC", ajasttz="local"):
    """ from epochtime to datetime object return
        ex 1367901256123 => dateime(2013,5,7,13,34,16,123000)
        Attention !! epoch is Expected UTC time!!
    """
    epoch = int(epoch)
    secs = epoch/1000
    mils = epoch - (secs*1000)
    if inptz.lower() == "utc":
        dt = datetime.utcfromtimestamp(secs)
    else:
        dt = datetime.fromtimestamp(secs)
    return dt.replace(tzinfo=_gettzinfo(inptz), microsecond=mils*1000).astimezone(_gettzinfo(ajasttz))

def to_epoch(s, inptz="local"):
    """ from str or date, time object to timestamp (millisecond) convert
        ex. "2013/05/07 13:34:16.123" => 1367901256123 (int)
    """
    dt = to_dt(s, inptz, "local")
    return int((mktime(dt.timetuple()) *1000) + (dt.microsecond/1000))

def to_ts(s, inptz="local"):
    """ from str or date, time object to timestamp (second) convert
        ex. "2013-05-07 13:34:16" => 1367901256 (int)
    """
    dt = to_dt(s, inptz, "local")
    return int((mktime(dt.timetuple())))

# def to_ts(s, inptz="local", ajasttz="UTC"):
#     """ from str or date, time object to timestamp (second) convert
#         ex. "2013-05-07 13:34:16" => 1367901256 (int)
#     """
#     dt = to_dt(s, inptz=inptz)
#     ret = to_epoch(dt, inptz) / 1000
#     if ajasttz.lower() == "utc":
#         return ret
#     else:
#         return ret + dt.utcoffset().seconds

def from_ts(s, inptz="UTC", ajasttz="local"):
    """ from timestamp to datetime object return
        ex 1367901256 => dateime(2013,5,7,13,34,16,0)
    """
    ts = int(s)
    try:
        return from_epoch(ts*1000, inptz).replace(tzinfo=_gettzinfo(ajasttz))
    except ValueError:
        return from_epoch(ts, inptz).replace(tzinfo=_gettzinfo(ajasttz))

def to_simple(s, inptz="local", ajasttz="local"):
    """ from date string to 'YYYY-mm-dd 24HH:MM:SS'
        ex. "2013-05-07 13:34:16"
    """
    return to_dt(s, inptz, ajasttz).strftime("%Y-%m-%d %H:%M:%S")

def to_isosec(s, inptz="local", ajasttz="UTC"):
    """ RFC3339 for second
        ex. "2013/05/07 13:34:16" => "2013-05-07T04:34:16Z"
    """
    dt = to_dt(s, inptz, ajasttz)
    ret = dt.isoformat()[:19]
    zone = dt.strftime("%z").replace("+0000","Z")
    return ret + zone

def to_iso(s, inptz="local", ajasttz="UTC"):
    """ RFC3339 
        ex. "2013/05/07 13:34:16.123" => "2013-05-07T04:34:16.123Z"
    """
    dt = to_dt(s, inptz, ajasttz)
    if dt.microsecond == 0:
        return to_isosec(s, inptz=inptz, ajasttz=ajasttz)
    ret = dt.isoformat()[:23]
    zone = dt.strftime("%z").replace("+0000","Z")
    return ret + zone

def to_ctime(s, inptz="local", ajasttz="local"):
    """ Legacy Defact Standard Format. Unix ctime format
        ex. "2013/05/07 13:34:16.123" => "Tue May  7 13:34:16 2013"
    """
    return to_dt(s, inptz, ajasttz).strftime("%c").strip()

def to_htime(s, inptz="local", ajasttz="local"):
    """ Legacy Defact Standard Format. HTML, EMail use date format
        RFC 2822
    """
    dt = to_dt(s, inptz, ajasttz)
    return dt.strftime("%a, %d %b %Y %H:%M:%S %z")

def tzdictionary_maintanance():
    from io import StringIO
    import re
    from charutils import tounicode
    from ioutils import asynccsv, asyncZip

    with asyncZip("http://download.geonames.org/export/dump/cities15000.zip") as czip:
        sio = StringIO(czip.read("cities15000.txt"))
    f = asynccsv.open(sio, delimiter="\t", lineterminator="\n")
    dic = {}
    
    def norm(data):
        return re.sub("[\-_. \'!#\$%&\+\*\?<>\(\)=]","",tounicode(data,"utf_8")[0].lower())
     
    for row in f:
        tz = row[-2]
        dic[tz.lower()]   = tz
        dic[norm(row[1])] = tz
        dic[norm(row[7])] = tz
        dic[norm(row[8])] = tz
    
    with open(dirname(__file__) + "/dat/tzdict.json", "wb") as tzdic:
        json.dump(dic, tzdic)
    f.close()

if __name__ == '__main__':
    def _testto_dt():
        dto = datetime(2013,5,7,13,34,16,123000,tzinfo=tzlocal())
        forms =(("normal","2013/05/07"),
                ("exception","13/05/07"),
                ("normal","05/07/2013"),
                ("exception","05/2013"),
                ("normal","2013.05.07"),
                ("normal","20130507T133416+0000"),
                ("normal","2013-05-07T13:34:16+0000"),
                ("normal","2013-05-07T13:34:16.000+0000"),
                ("normal","2013-05-07T13:34:16.000000+0000"),
                ("normal","2013-05-07"),
                ("normal","2013.05.07"),
                ("exception","07.05.2013"),
                ("normal","Tue, 07 May 2013 13:34:16 +0900"),
                ("normal","Tue May  7 13:34:16 2013"))
        for expected,inpstr in forms:
            parseret = to_dt(inpstr)
            if expected == "normal":
                assert parseret.date() == dto.date()
            elif expected == "exception":
                assert parseret.date()
        # Test Adjast Timezone
        dts = "2013-05-07T13:34:16+0900"
        res = to_dt(dts)
        assert res.tzinfo == tzlocal()
        assert res.utctimetuple().tm_hour == 4
        dts = "2013-05-07T13:34:16Z"
        res = to_dt(dts)
        assert res.utctimetuple().tm_hour == 13
        dts = "2013-05-07T13:34:16+0900"
        res = to_dt(dts,adjusttz="UTC")
        assert res.isoformat() == "2013-05-07T04:34:16+00:00"
    
    def _testdt_from_epoch():
        assert from_epoch(1367901256123).utctimetuple().tm_hour == 4
        assert from_epoch(1367901256123) == datetime(2013,5,7,13,34,16,123000,tzinfo=tzlocal()) 
    
    def _testto_epoch():
        assert to_epoch("2013/05/07 13:34:16.123") == 1367901256123
    
    def _testto_ts():
        assert to_ts("2013/05/07 13:34:16.123") == 1367901256
    
    def _testfrom_ts():
        assert from_ts(1367901256) == datetime(2013,5,7,13,34,16,tzinfo=tzlocal())
    
    def _testto_simple():
        assert to_simple(from_ts(1367901256)) == "2013-05-07 13:34:16"
    
    def _testto_isosimple():
        assert to_isosec("2013-05-07 13:34:16") == "2013-05-07T04:34:16Z"
    
    def _testto_iso():
        assert to_iso("2013-05-07 13:34:16.123") == "2013-05-07T04:34:16.123Z"
    
    def _testto_ctime():
        assert to_ctime("2013-05-07 13:34:16.123") == "Tue May  7 13:34:16 2013"
    
    def _testto_htime():
        assert to_htime("2013-05-07 13:34:16.123") == "Tue, 07 May 2013 13:34:16 +0900"
    
    def test():
        _testto_dt()
        _testdt_from_epoch()
        _testto_epoch()
        _testto_ts()
        _testfrom_ts()
        _testto_simple()
        _testto_isosimple()
        _testto_iso()
        _testto_ctime()
        _testto_htime()

    test()
#     tzdictionary_maintanance()

