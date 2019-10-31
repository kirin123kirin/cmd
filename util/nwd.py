#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from tempfile import gettempdir
import subprocess
import types

import blockdiag
_font = os.path.join(os.path.dirname(blockdiag.__file__), "tests", "VLGothic", "VL-Gothic-Regular.ttf")

from blockdiag.utils.bootstrap import Application
from blockdiag.utils.logging import error
import nwdiag
import nwdiag.builder
import nwdiag.drawer
import nwdiag.parser
from nwdiag.parser import Diagram, Network, Group, Node, Attr, Edge, Peer, Route, Extension, Statements
from functools import lru_cache
from itertools import groupby
from operator import itemgetter, attrgetter

from util import readrow, to_hankaku
from util.nw import getipinfo


class NwdiagApp(Application):
    module = nwdiag
    code = ""
    outtmp = os.path.join(gettempdir(), "tmp_nwdiag.")
    _options = types.SimpleNamespace(
        antialias=True,
        config=None,
        debug=None,
        output=None,
        font=[_font],
        fontmap=None,
        ignore_pil=False,
        transparency=True,
        size=None,
        type="SVG",
        nodoctype=None,
        input="",
        )

    @property
    def options(self):
        if self._options.output:
            ext = os.path.splitext(self._options.output)[-1][1:].upper()
            if self._options.type != ext:
                self._options.type = ext
        else:
            self._options.output = self.outtmp + self._options.type.lower()

        return self._options

    def writediag(self, parsed_diag, outpath=None):
        if outpath:
            self.options.output = outpath

        try:
            self.create_fontmap()
            self.setup()
            return self.build_diagram(parsed_diag)
        except SystemExit as e:
            return e
        except UnicodeEncodeError:
            error("UnicodeEncodeError caught (check your font settings)")
            return -1
        finally:
            self.cleanup()

def normadr(ip, sub=""):
    r = "{}/{}".format(ip, sub).rstrip("/").replace("//", "/")
    if r.startswith("/"):
        return ""
    return r

def cleansing(rows):
    _cleansing = {
        "システム": "group_id",
        "system": "group_id",

        "セグメント": "network_id",
        "ネットワーク": "network_id",
        "nw": "network_id",
        "segment": "network_id",

        "サーバ": "_server_node_id",
        "server": "_server_node_id",

        "ホスト": "_host_node_id",
        "host": "_host_node_id",

        "ipアドレス": "_ip",
        "ip": "_ip",

        "networkaddress": "_network_address",
        "ネットワークアドレス": "_network_address",

        "プレフィクス": "_prefix",
        "ビットマスク": "_prefix",
        "prefix": "_prefix",
        "bitmask": "_prefix",

        "サブネットマスク": "_subnet",
        "サブネット": "_subnet",
        "ネットマスク": "_subnet",
        "subnetmask": "_subnet",
        "subnet": "_subnet",
        "netmask": "_subnet"
    }

    _cleansing = {to_hankaku(k): v for k, v in _cleansing.items()}

    flagattr = False

    if hasattr(rows, "__next__"):
        _head = next(rows)
    else:
        _head = rows.pop(0)

    if hasattr(_head, "value"):
        _head = _head.value
        flagattr = True

    cleaned = [to_hankaku(h).lower().replace(" ", "").replace("名", "") for h in _head]
    head = [_cleansing.get(h, h) for h in cleaned]
    ret = []

    for row in map(attrgetter("value"), rows) if flagattr else rows:
        r = dict(zip(head, row))
        get = lambda x: r.pop(x, "")

        ip, subnet, pref = get("_ip"), get("_subnet"), get("_prefix")
        server, host = get("_server_node_id"), get("_host_node_id")

        if not server and not host:
            continue

        r["node_id"] = "{}\n{}".format(server, host).replace("\n()", "")
        r["network_value"] = ""
        r["node_value"] = ip

        if "network_id" not in r:
            r["network_id"] = ""

        try:
            nwa = get("_network_address")
            if nwa:
                nwadr = getnwadr(normadr(nwa, pref or subnet))
            else:
                nwadr = getnwadr(normadr(ip, pref or subnet))

            r["network_id"] += "\n" + nwadr
            if subnet:
                r["network_id"] += "\r\n" + subnet

            if ip == nwadr.split("/")[0]:
                raise ValueError("{} is Network Address.".format(ip))


        except ValueError as e:
            r["network_id"] = "ERR: " + str(e)


        if r["network_id"].strip() and r["node_id"].strip():
            ret.append(r)

    return ret

@lru_cache()
def getnwadr(ipa):
    return getipinfo(ipa, lambda x: "{}/{}".format(x.nwadr, x.bitmask))

def parse(rows):
    rows = cleansing(rows)

    networks = []

    groups = []

    for (nwid, nwval), row in groupby(rows, itemgetter("network_id", "network_value")):
        n = Network(id=nwid, stmts=[Attr(name='address', value=nwval)])
        add = n.stmts.append

        for ndid, r in groupby(row, itemgetter("node_id")):
            node = Node(id=ndid, attrs=[Attr(name="address", value=", ".join(x["node_value"] for x in r))])
            add(node)

        networks.append(n)

    try:
        for gid, row in groupby(rows, itemgetter("group_id")):
            if gid:
                groups.append(
                    Group(id=gid, stmts=[Attr(name='label', value=gid),
                        Attr(name='fontsize', value="16"),
                        Attr(name='textcolor', value="#FF0000"),
                        *[Node(id=r["node_id"], attrs=[]) for r in row],
                    ]))
    except KeyError:
        pass

    return Diagram(id=['nwdiag', None], stmts=[*groups, *networks])


def render(rows, outpath=None, outtype="SVG", autoopen=True):
    diag = parse(rows)

    if diag.stmts:
        app = NwdiagApp()
        if outpath:
            app.options.type = outtype
        app.writediag(diag, outpath=outpath)
        print("OUT: " + app.options.output)
    else:
        raise ValueError("empty input data?")

    if autoopen:
        if os.name == "nt":
            cmd = "start "
        else:
            cmd = "open "

        subprocess.check_call(cmd + app.options.output, shell=True)

def test():
    rows = [['システム名', 'セグメント名', 'サーバ名', 'ホスト名', 'ipアドレス', 'プレフィクス', 'サブネットマスク'],
 ['なんでやねｎ', 'seg', 'server1', 'host1', '10.111.111.0', '28', '255.255.255.0'],
 ['nandeyanen', 'seg', 'server2', 'host2', '10.111.111.1', '28', '255.255.255.0'],
 ['nandeyanen', 'seg', 'server2', 'host2', '10.111.111.2', '28', '255.255.255.0'],
 ['', 'seg', 'server4', '', '10.111.111.3', '', '']]

    diag = parse(rows)
    test1(diag)

def test1(diag):
    import time
    now = time.time()
    app = NwdiagApp()
    app.writediag(diag, "./test.png")

    assert(os.stat(app.options.output).st_mtime > now)
    if os.name == "nt":
        subprocess.check_call("start " + app.options.output, shell=True)

def main():
    import sys
    from argparse import ArgumentParser
    ps = ArgumentParser(prog=os.path.basename(sys.argv[0]), description="ipaddress to nwaddress infomation program\n")
    padd = ps.add_argument
    padd('-t', '--type', type=str, default="SVG",
         help='output image file type SVG, or PNG or PDF')
    padd('-o', '--outputfile', type=str, default=None,
         help='Output data filepath')

    padd('-q', '--quiet', action="store_true", default=False,
         help='quiet mode (Stop Auto open)')

    args = ps.parse_args()

    rows = readrow.clipboard("csv")

    render(rows, outpath=args.outputfile, outtype=args.type, autoopen=not args.quiet)

if __name__ == "__main__":
    main()
#    test()

