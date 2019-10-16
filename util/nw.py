#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# The _XMLHandler class is:
# Copyright (c) Martin Blech
# Released under the MIT license.
# see https://opensource.org/licenses/MIT
__author__  = 'm.yama'
__license__ = 'MIT'
__date__    = 'Wed Jul 31 17:35:47 2019'
__version__ = '0.0.1'

from collections import namedtuple
from ipaddress import ip_interface
import re

ZEN = "".join(chr(0xff01 + i) for i in range(94))
HAN = "".join(chr(0x21 + i) for i in range(94))

def to_hankaku(s):
    return s.translate(str.maketrans(ZEN, HAN))

rev4 = re.compile("([\d\.\:]{7,})[^\d]*([\d\.\:]{1,15})")

ipinfo = namedtuple("IPinfo", ["ipadr", "netmask", "bitmask", "nwadr", "numip", "broadcast"])

def getipinfo(
    string,
    callback=lambda x: "{nwadr}\t{netmask}(/{bitmask})".format(**x._asdict()),
    ):

    if not string:
        raise ValueError("Nothing data.")

    s = to_hankaku(str(string)).strip()
    if not s and s.count(".") < 3 and s.count(":") < 2:
        raise ValueError("Not IPaddress string. -> `{}`".format(string))

    try:
        iface = ip_interface("{}/{}".format(*rev4.findall(s)[0]))
    except (IndexError, ValueError):
        iface = ip_interface(s)

    bit = int(str(iface).split("/")[1])
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


def test():
    assert getipinfo("192.168.1.1/27")
    assert getipinfo("192.168.1.1/255.255.255.240")
    try:
        getipinfo("   ")
        getipinfo(None)
        getipinfo("192.168.1.0")
    except ValueError:
        pass

def main():
    import os
    import sys
    from pyperclip import paste as getclip, copy as clip
    from argparse import ArgumentParser

    ps = ArgumentParser(prog=os.path.basename(sys.argv[0]),
                        description="ipaddress to nwaddress infomation program\n")
    padd = ps.add_argument

    padd('-V','--version', action='version', version='%(prog)s ' + __version__)
    
    padd('-f', '--form', type=str, default="{nwadr}\t{netmask}(/{bitmask})",
         help='Usable Keyword formating {}'.format(tuple("{" + x + "}" for x in ipinfo._fields)))
    
    args = ps.parse_args()
    
    form = args.form

    func = lambda x: form.format(**x._asdict())
    
    lines = getclip().splitlines()
    
    ret = []
    
    for line in lines:
        try:
            ret.append(getipinfo(line, callback=func))
        except ValueError as e:
            ret.append(e.args[0])

    clip("\r\n".join(ret))

if __name__ == "__main__":
#    test()
    main()
