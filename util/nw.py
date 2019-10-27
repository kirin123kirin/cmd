#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Released under the MIT license.
# see https://opensource.org/licenses/MIT

__author__  = 'm.yama'
__license__ = 'MIT'
__date__    = 'Oct 18 17:35:47 2019'
__version__ = '0.0.2'

__all__ = [
    "isin_nw",
    "getipinfo",
    "tokenip",
    "formatip",
]

from collections import namedtuple
from ipaddress import ip_interface
import re
from itertools import zip_longest

FORMAT = "{nwadr}/{bitmask}\t{netmask}"

ZEN = "".join(chr(0xff01 + i) for i in range(94))
HAN = "".join(chr(0x21 + i) for i in range(94))

def to_hankaku(s):
    return s.translate(str.maketrans(ZEN, HAN))

rev4 = re.compile(r"(?:(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]\d|\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]\d|\d)(?:[/:][\.:\d]+)?")

ipinfo = namedtuple("IPinfo", ["ipadr", "netmask", "bitmask", "nwadr", "numip", "broadcast"])


def normip(string):
    if not string:
        raise ValueError("Nothing data.")

    s = to_hankaku(str(string)).strip()
    if not s and s.count(".") < 3 and s.count(":") < 2:
        raise ValueError("Not IPaddress string. -> `{}`".format(string))

    try:
        return ip_interface("{}/{}".format(*rev4.findall(s)[0]))
    except (IndexError, ValueError):
        return ip_interface(s)

def isin_nw(ipadr, nwadr):
    ip = normip(ipadr).ip
    nw = normip(nwadr).network

    if ip == nw.network_address:
        raise ValueError("IP address is Network Address?")

    return ip in nw

def getipinfo(string, callback=None):

    iface = normip(string)
    s = str(iface)

    bit = int(s.split("/")[1])
    ipa = str(iface.ip)
    nw = iface.network
    nwa = str(nw.network_address)

    if bit == 32:
        raise ValueError("No Subnet Info? -> {}".format(s))

    ret = ipinfo(
        None if ipa == nwa else ipa,  # IPaddress
        str(nw.netmask),  # SubnetMask
        bit,              # BitMask
        nwa,              # Network Address
        nw.num_addresses, # count IP address num
        str(nw[-1])       # BroadCast Address
    )

    if callback:
        return callback(ret)
    else:
        return ret

def tokenip(
        data,
        callback=lambda x: FORMAT.format(**x._asdict())):

    nons, mats = rev4.split(data), rev4.findall(data)

    for non, mat in zip_longest(nons, mats, fillvalue=""):
        try:
            yield non, getipinfo(mat, callback), None
        except ValueError as e:
            yield non, mat, e.args[0]

def formatip(data,
    callback=lambda x: FORMAT.format(**x._asdict())):

    ret = ""
    for n, m, err in tokenip(data, callback=callback):
        ret += n + m
    return ret


extractip = rev4.findall


def test():
    assert getipinfo("192.168.1.1/27")
    assert getipinfo("192.168.1.1/255.255.255.240")
    try:
        getipinfo("   ")
        getipinfo(None)
        getipinfo("192.168.1.0")
    except ValueError:
        pass

    r=formatip(data = "hoge server 192.168.1.1 192.168.1.0/24")
    assert r.startswith("hoge server ")

    r = formatip("""Ethernet adapter ローカル エリア接続:

        Connection-specific DNS Suffix  . : example.co.jp
        IP Address. . . . . . . . . . . . : 192.168.1.40
        Subnet Mask . . . . . . . . . . . : 255.255.255.0 ……サブネットマスク
        Default Gateway . . . . . . . . . : 192.168.1.2/24""")
    assert "ローカル エリア接続" in r

def main():
    import os
    import sys
    from io import StringIO
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

    padd('-f', '--form', type=str, default=FORMAT,
         help='Usable Keyword formating {}'.format(tuple("{" + x + "}" for x in ipinfo._fields)))

    padd('-w', '--withorgdata', action="store_true",
         help='Output data with original data')

    args = ps.parse_args()

    callback = lambda x: args.form.format(**x._asdict())

    wd = args.withorgdata

    with StringIO() as ret:

        lines = StringIO(getclip())

        for line in lines:
            for n, m, err in tokenip(line, callback=callback):
                if err is None or err.startswith("Nothing data"):
                    ret.write("{}{}".format(n if wd else "", m))
                else:
                    ret.write("{}{}\t<- [ERROR]{}".format(n if wd else "", m, err))

            if not wd:
                ret.write("\n") #TODO last line in "\n"


        setclip(ret.getvalue())

if __name__ == "__main__":
#    test()
    main()
