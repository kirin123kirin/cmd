#!/usr/bin/env python
## -*- coding: utf-8 -*-

import re
import time
import datetime

# Dependencies Require Modules
from dateutil.parser import parse as dateparse

__author__  = "m.yama"
__version__ = "0.1.3"

__all__ = ['isdate', 'isemail', 'ishex', 'ishtml', 'isipaddr',
           'ispassword', 'isurl', 'isxml', 're_email', 're_hexvalue',
           're_html', 're_ipaddr', 're_password', 're_url', 're_xml']


_urischemes = 'gopher|aaas?|about|acap|afp|aim|apt|bolo|bzr|callto|cap|cid|coffee|crid|cvs|daap|data|dav|dict|dns|dsnp|ed2k|fax|feed|file|fish|ge?o|gg|git|gizmoproject|h323|https?|iax|im|imap|info|irc|ircs|itms|javascript|ldaps?|magnet|mailto|mid|mms|msnim|news|nfs|nntp|pop|postal2|pres|rsync|rtsp|[st]?ftp|secondlife|sips?|skype|sm[bs]|snmp|source|spotify|ssh|steam|svn|tag|tel|telnet|urn|view|wais|webcal|winamp|ws|wyciwyg|xfire|xmpp|ymsgr'
_gtlds = 'aero|arpa|asia|biz|cat|com|coop|edu|gov|info|int|jobs|mil|mobi|museum|name|net|org|pro|tel|travel|a[cd-gil-q-uwxz]|b[abd-jm-or-tvwyz]|c[acdf-ik-orsuvx-z]|d[dejkmoz]|e[ceghr-u]|f[ijkmor]|g[abd-il-np-uwy]|h[kmnrtu]|i[del-oq-t]|j[emop]|k[eg-imnprwyz]|l[a-cikr-vy]|m[ac-eghk-z]|n[acefgilopruz]|om|p[aef-hk-nr-twy]|qa|r[eosuw]|s[a-eg-ort-vyz]|t[c-prtv-z]|u[agkmsyz]|v[aceginu]|w[fs]|y[etu]|z[amrw]'

re_password = re.compile('[^\r\n]{6,18}',re.IGNORECASE)
re_hexvalue = re.compile('#?([a-f0-9]{6}|[a-f0-9]{3})',re.IGNORECASE)
re_email    = re.compile('([a-z0-9_\.-]+)@([\da-z\.-]+)\.(%s)' % _gtlds)
re_url      = re.compile('(%s):\/\/(?:(localhost|[\da-z\.-]+(?:\.(%s))|[\d\.]+)):?([/\?].*|\d+)*$' % (_urischemes, _gtlds))
re_ipaddr   = re.compile('(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)')
re_html     = re.compile('^\s*(<!?[^<>]+>)(.*</[^<>]+)>\s*',re.DOTALL)
re_xml      = re.compile('^\s*<\?xml\s+version\s*=\s*([\'"\d.]+)\s+(?:encoding\s*=\s*[\'"]([^\s]+)[\'"]|[^<>\s]+)?\s*\?>(.*</[^<>\d\.\-][^\s]*>)\s*',re.DOTALL)
re_pickle   = re.compile('^[\x80FGI-NP-VXa-eg-jlo-u0-2\.\(\}\]\)].*\.$',re.DOTALL)

def check_value(data): #TODO
    if isinstance(data, str):
        pass

def ispassword(data):
    return re_password.match(data) is not None

def ishex(data):
    return re_hexvalue.match(data) is not None

def isemail(data):
    return re_email.match(data) is not None

def isurl(data):
    return re_url.match(data) is not None

def isipaddr(data):
    return re_ipaddr.match(data) is not None

def ishtml(data):
    return re_html.match(data) is not None and re_xml.match(data) is None

def isxml(data):
    return re_xml.match(data) is not None

def isdate(s):
    if isinstance(s, datetime.datetime) or isinstance(s, time.struct_time):
        return True
    elif isinstance(s, str):
        try:
            return dateparse(s) is not None
        except:
            return False
    else:
        return False

def ispickle(s):
    return re_pickle.match(s) is not None

def _testispassword():
    assert ispassword("hogehoge48943") is True
    assert ispassword("hoge") is False
    assert ispassword("hoge\ndfsa458") is False


def _testishex():
    assert ishex("00000") is True
    assert ishex("FFFFF") is True
    assert ishex("GGGGG") is False

def _testisemail():
    assert isemail("example@example.com") is True
    assert isemail("example.222@example.spam.com") is True
    assert isemail("example@example") is False
    assert isemail("example@example.hoge") is False
    assert isemail("example/example.hoge") is False

def _testisurl():
    assert isurl("http://www.google.com") is True
    assert isurl("https://www.google.com/index.html") is True
    assert isurl("https://www.google.co.jp") is True
    assert isurl("https://www.google.co.uk") is True
    assert isurl("https://www.google.co.us") is True
    assert isurl("https://localhost") is True
    assert isurl("https://www.hoge.foofdsafas") is False
    assert isurl("htp://www.google.com") is False
    assert isurl("ftp://www.google.com") is True
    assert isurl("feed://www.google.com") is True
    assert isurl("feeds://www.google.com") is False
    assert isurl("ftp://127.0.0.1:21") is True
    assert re_url.search("ftp://127.0.0.1:21").groups() == ("ftp","127.0.0.1",None,"21")

def _testishtml():
    x1 = "http://www.sotechsha.co.jp/xml/sample.xml"
    x2 = "http://www.w3schools.com/xml/note.xml"
    h1 = "http://www.google.com/index.html"
    import urllib.request, urllib.parse, urllib.error
    for x in (x1,x2):
        res = urllib.request.urlopen(x)
        assert ishtml(res.read()) is False
        res.close()
    for h in (h1,):
        res = urllib.request.urlopen(h)
        assert ishtml(res.read()) is True
        res.close()

def _testisxml():
    x1 = "http://www.sotechsha.co.jp/xml/sample.xml"
    x2 = "http://www.w3schools.com/xml/note.xml"
    h1 = "http://www.google.com/index.html"
    strange_sample = """<?xml version="1.0" encoding="UTF-8" ?>
<ResultSet xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="urn:yahoo:jp:jlp:DAService" xsi:schemaLocation="urn:yahoo:jp:jlp:DAService http://jlp.yahooapis.jp/DAService/V1/parseResponse.xsd">
<Result>
<ChunkList>
<Chunk>
<Id>0</Id>
<Dependency>-1</Dependency>
<MorphemList>
<Morphem>
<Surface>山田</Surface><Reading>やまだ</Reading><Baseform>山田</Baseform><POS>名詞</POS><Feature>名詞,地名,*,山田,やまだ,山田</Feature>
</Morphem>
<Morphem>
<Surface>太郎</Surface><Reading>たろう</Reading><Baseform>太郎</Baseform><POS>名詞</POS><Feature>名詞,名詞人,*,太郎,たろう,太郎</Feature>
</Morphem>
</MorphemList>
</Chunk>
</ChunkList>
</Result>
</ResultSet>"""
    import urllib.request, urllib.parse, urllib.error
    for x in (x1,x2):
        res = urllib.request.urlopen(x)
        assert isxml(res.read()) is True
        res.close()
    for h in (h1,):
        res = urllib.request.urlopen(h)
        assert isxml(res.read()) is False
        res.close()
    assert isxml(strange_sample) == True

def _testisdate():
    dto = datetime.datetime.now()
    forms = ["%Y/%m/%d",
            "%y/%m/%d",
            "%m/%d/%Y",
            "%m/%Y",
            "%Y/%m/%d",
            "%Y.%m.%d",
            "%Y%m%dT%H%M%S%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.000%z",
            "%Y-%m-%dT%H:%M:%S.%s%z",
            "%Y-%m-%d",
            "%Y.%m.%d",
            "%d.%m.%Y",
            "%a, %d %b %Y %H:%M:%S +0000",
            "%c"]
    for f in forms:
        parseret = None
        try:
            dtstr = datetime.datetime.strftime(f, dto.timetuple())
            parseret = dateparse(dtstr)
            if str(parseret.date()) == str(dto.date()):
                ("OK:", dtstr,"=>", parseret.isoformat())
                assert isdate(dtstr) is True
                assert isdate(parseret) is True
            else:
                raise
        except:
            if parseret:
                ("Parser Misstake:", f, parseret)
            else:
                ("Parser Can't Parse:" ,f)
    assert isdate(["2011/03/11"]) is False
    assert isdate(["20111/03/11"]) is False
    assert isdate(["2011/03/32"]) is False
    assert isdate(["0011/03/11"]) is False
    assert isdate(["2011/13/11"]) is False
    assert isdate(["2011/02/29"]) is False

def _testis_pickle():
    import pickle
    from io import StringIO
    f = StringIO()
    pickle.dump("hoge", f,protocol=2)
    assert ispickle(f.getvalue())
    f.truncate(0)
    pickle.dump("hoge", f,protocol=1)
    assert ispickle(f.getvalue())
    f.truncate(0)
    pickle.dump("hoge", f,protocol=0)
    assert ispickle(f.getvalue())
    f.truncate(0)
    f.write("String")
    assert ispickle(f.getvalue()) is False

if __name__ == "__main__":
    _testispassword()
    _testishex()
    _testisemail()
    _testisurl()
    _testishtml()
    _testisxml()
    _testisdate()
    _testis_pickle()
    
