#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Released under the MIT license.
# see https://opensource.org/licenses/MIT

__author__  = 'm.yama'
__license__ = 'MIT'
__date__    = 'Oct 18 17:35:47 2019'
__version__ = '0.0.4'

__all__ = [
    "isin_nw",
    "getipinfo",
    "formatip",
]

import re
from collections import namedtuple
from ipaddress import ip_interface, ip_address

from util.core import to_hankaku

SEP = "\t"
FORMAT = SEP.join(["{nwadr}/{bitmask}", "{netmask}","{numip}IP", "{iprange}"])
ipinfo = namedtuple("IPinfo", ["ipadr", "netmask", "bitmask", "nwadr", "numip", "broadcast", "iprange", "nwrange"])


_PATTERN_IPADDR_UNIT = "(?:25[0-4]|2[0-4]\d|1\d{2}|0?[1-9]\d|0{,2}\d)"
_PATTERN_NETMASK_UNIT = "(?:255|254|248|240|224|192|128|0{1,3})"
_PATTERN_IPADDR = "((?:{0}\.){{3}}(?:{0}))".format(_PATTERN_IPADDR_UNIT)
_PATTERN_PREFIX = "\(?[:/]?([1-2]\d|3[12]|[89])\)?"
_PATTERN_NETMASK = "\(?/?(255\.255\.255\.{0}|255\.255\.{0}\.0{{1,3}}|255\.{0}\.0{{1,3}}\.0{{1,3}})\)?".format(_PATTERN_NETMASK_UNIT)
DIC_PREFMASK = {
    "8": "255.0.0.0",
    "9": "255.128.0.0",
    "10": "255.192.0.0",
    "11": "255.224.0.0",
    "12": "255.240.0.0",
    "13": "255.248.0.0",
    "14": "255.252.0.0",
    "15": "255.254.0.0",
    "16": "255.255.0.0",
    "17": "255.255.128.0",
    "18": "255.255.192.0",
    "19": "255.255.224.0",
    "20": "255.255.240.0",
    "21": "255.255.248.0",
    "22": "255.255.252.0",
    "23": "255.255.254.0",
    "24": "255.255.255.0",
    "25": "255.255.255.128",
    "26": "255.255.255.192",
    "27": "255.255.255.224",
    "28": "255.255.255.240",
    "29": "255.255.255.248",
    "30": "255.255.255.252",
    "31": "255.255.255.254",
    "32": "255.255.255.255"
}

repad = re.compile("(\.)0*(\d+)")

is_ipaddr = re.compile(_PATTERN_IPADDR).search
is_prefix = re.compile(_PATTERN_PREFIX).search
is_netmask = re.compile(_PATTERN_NETMASK).search
is_addrzeropadding = repad.search

prefix_to_mask = DIC_PREFMASK.get
mask_to_prefix = {v:k for k, v in DIC_PREFMASK.items()}.get
del_zeropadding = repad.sub

rev4=re.compile("{0}\s*(?:{2}\s*{1}|{1}\s*{2}|{2}|{1})?".format(_PATTERN_IPADDR, _PATTERN_PREFIX, _PATTERN_NETMASK))

def unicode_escape(x):
    return x.encode().decode("unicode_escape")

def extractip(string, callback=None):
    """
    [Parameter]:
        string: IPアドレスとサブネットマスク or Nビットマスクを含む文字列
        callback: 戻り値を変換して出したい場合はコールバック関数を入れる（引数にはIPv4Interfaceオブジェクトが入る）
          * IPv4Interfaceオブジェクトとは->https://docs.python.org/ja/3/library/ipaddress.html#ipaddress.IPv4Interface

    [Return]:
        [callback引数なし(None)の場合]
            リスト: IPv4Interface インスタンス
        [callback引数ありの場合]
            リスト: callback関数適用後の結果

    [Example]
        > extractip("192.168.100.1  255.255.255.0(/24)")
        [IPv4Interface('192.168.100.1/24')]

        > extractip("192.168.100.1  255.255.255.0(/24)", lambda x: str(x.network))
        ['192.168.100.0/24']
    """
    if not string:
        return []

    ret = []

    if callback:
        add = lambda x: ret.append(callback(x))
    else:
        add = ret.append

    for dat in rev4.findall(to_hankaku(string)):
        ip, prefix, netmask = None, None, None

        for x in dat:
            if not x:
                continue
            if is_addrzeropadding(x):
                x = del_zeropadding(r"\1\2", x)

            if len(x) < 3:
                if is_prefix(x):
                    prefix = x
            else:
                if is_netmask(x):
                    netmask = x
                elif is_ipaddr(x):
                    ip = x

        if prefix and netmask:
            msk = prefix_to_mask(prefix)
            if msk == netmask:
                add( ip_interface(ip + "/" + prefix) )
            else:
                raise ValueError(
                    "サブネットマスク`{}` は `{}` ビットマスクではない。もしかして`{}`の誤り?".format(
                        netmask, prefix, msk))
        elif prefix:
            add( ip_interface(ip + "/" + prefix) )
        elif netmask:
            add( ip_interface(ip + "/" + netmask) )
        else:
            raise ValueError("{} is Unknown Subnetmask or Prefix".format(ip))

    return ret


def isin_nw(ipadr, nwadr):
    ip = ip_address(ipadr)
    nw = extractip(nwadr, lambda x: x.network)[0]

    if ip == nw.network_address:
        raise ValueError("IP address is Network Address?")

    return ip in nw


def getipinfo(string, callback=None):
    ifaces = extractip(string)

    for iface in ifaces:
        s = str(iface)

        bit = int(s.split("/")[1])
        if bit < 8 or bit > 32:
            raise ValueError("No validation SubnetMask Value -> {}".format(s))

        ipa = str(iface.ip)
        nw = iface.network
        nwa = str(nw.network_address)
        nwnum = nw.num_addresses

        nwsplit = nwa.split(".")
        ipfw, iprf = (".".join(nwsplit[:-1]) ,int(nwsplit[-1]))
        ipfirst = "{}.{}".format(ipfw, iprf + 1)
        iplast = "{}.{}".format(ipfw, iprf + nwnum - 2)
        nwfirst = "{}.{}".format(ipfw, iprf)
        nwlast = "{}.{}".format(ipfw, iprf + nwnum)

        ret = ipinfo(
            None if ipa == nwa else ipa,  # IPaddress
            str(nw.netmask),  # SubnetMask
            bit,              # BitMask
            nwa,              # Network Address
            nwnum, # count IP address num
            str(nw[-1]),       # BroadCast Address
            "{} - {}".format(ipfirst, iplast), #Valid Range IPAddress
            "{} - {}".format(nwfirst, nwlast), #Valid Range IPAddress
        )

        if callback:
            yield callback(ret)
        else:
            yield ret


def formatip(string,
        callback=lambda x: FORMAT.format(**x._asdict())):

    for mat in rev4.finditer(string):
        prev, s, fwd = string[:mat.start()], mat.string, string[mat.end():]

        try:
            r = next(getipinfo(s, callback))
            yield "{}{}{}".format(prev, r, fwd)
        except ValueError as e:
            yield "{}{}[<-ERROR {}]{}".format(prev, s, e, fwd)



def test():
    from datetime import datetime

    def test_getipinfo_simple():
        assert list(getipinfo("192.168.1.1/27"))
        assert list(getipinfo("192.168.1.1/255.255.255.240"))

    def test_getipinfo_expecterror():
        try:
            list(getipinfo("   "))
            list(getipinfo(None))
            list(getipinfo("192.168.1.0"))
        except ValueError:
            pass

    def test_getipinfo_withword():
        r=next(formatip("hoge server 192.168.1.1 192.168.1.0/24"))
        assert r.startswith("hoge server ")

    def test_getipinfo_withmultibytelines():
        r = formatip("""Ethernet adapter ローカル エリア接続:

            Connection-specific DNS Suffix  . : example.co.jp
            IP Address. . . . . . . . . . . . : 192.168.1.40
            Subnet Mask . . . . . . . . . . . : 255.255.255.0 ……サブネットマスク
            Default Gateway . . . . . . . . . : 192.168.1.2/24""")
        assert "ローカル エリア接続" in next(r)

    def test_isin_nw():
        assert(isin_nw("192.168.1.40", "192.168.1.0/24"))

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = datetime.now()
            func()
            t2 = datetime.now()
            print("{} : time {}".format(x, t2-t1))


def main():
    import os
    import sys
    from io import StringIO
    import codecs

    try:
        from pyperclip import paste as getclip, copy as setclip
    except ModuleNotFoundError:
        if os.name == "nt":
            import tkinter as tk
            def getclip():
                a=tk.Tk()
                return a.clipboard_get()

            import subprocess
            def setclip(text):
                if not isinstance(text, (str, int, float, bool)):
                    raise RuntimeError('only str, int, float, and bool values can be copied to the clipboard, not %s' % (text.__class__.__name__))
                text = str(text)
                p = subprocess.Popen(['clip.exe'],
                                     stdin=subprocess.PIPE, close_fds=True)
                p.communicate(input=text.encode("cp932"))

        else:
            getclip = ModuleNotFoundError("Please Install command: pip3 install pyperclip")
            setclip = ModuleNotFoundError("Please Install command: pip3 install pyperclip")

    from argparse import ArgumentParser

    ps = ArgumentParser(prog=os.path.basename(sys.argv[0]),
                        description="ipaddress to nwaddress infomation program\n")
    padd = ps.add_argument

    padd('-V','--version', action='version', version='%(prog)s ' + __version__)

    padd('-f', '--form', type=unicode_escape, default=FORMAT,
         help='Usable Keyword formating {}'.format(tuple("{" + x + "}" for x in ipinfo._fields)))

    padd('-s', '--sep',
         type=unicode_escape,
         help='output separator',
         default=SEP)

    padd('-H', '--header',
         action='store_true', default=False,
         help='print header')

    padd('-e', '--encoding',
        help='output encoding',
        default=os.name == "nt" and "cp932" or "utf-8")

    padd('-w', '--withorgdata', action="store_true",
         help='Output data with original data')

    padd('-c', '--clipboard',
         action='store_true', default=False,
         help='output to clipboard')

    padd('-o', '--outputfile', type=str, default=None,
         help='Output data filepath')

    padd("address",
         metavar="<address>",
         nargs="*",  default=None,
         help="IP or NW address/subnet")

    args = ps.parse_args()

    outfile = args.outputfile

    sep = args.sep

    encoding = args.encoding

    form = args.form if sep == SEP else args.form.replace(SEP, sep)

    callback = lambda x: form.format(**x._asdict())

    header = re.sub("[\{\}]", "", form if sep == SEP else form.replace(SEP, sep))

    func = formatip if args.withorgdata else getipinfo

    with codecs.open(outfile, "w", encoding=encoding) if outfile else StringIO() as ret:

        string = sep.join(args.address) if args.address else getclip()

        if not string:
            ps.print_usage(file=sys.stderr)
            print("\n", file=sys.stderr)
            raise ValueError("No Data.")

        if args.header:
            print(header, file=ret)

        for line in string.splitlines():
            for r in func(line, callback):
                print(r, file=ret)

        if not outfile:
            print(ret.getvalue())

        if args.clipboard:
            setclip(ret.getvalue()[:-1])


if __name__ == "__main__":
#    test()
    main()
