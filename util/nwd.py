#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from tempfile import gettempdir
import subprocess
import types

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
        antialias=None,
        config=None,
        debug=None,
        output=None,
        font=[os.path.join(os.path.dirname(nwdiag.__file__), "tests", "VLGothic", "VL-Gothic-Regular.ttf")],
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
        op = self._options
        if op.output is None:
            op.output = self.outtmp + op.type.lower()
        return op

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

        r["node_id"] = "{}\n({})".format(get("_server_node_id"), get("_host_node_id")).replace("\n()", "")
        r["node_value"] = ip

        ipa = "{}/{}".format(ip, pref or subnet ).rstrip("/").replace("//", "/")
        
        try:
            nwa = "/".join(map(str, getnwadr(ipa)))
            r["network_id"] += "\n" + nwa
            if subnet:
                r["network_id"] += "\n({})".format(subnet)
        except ValueError as e:
            r["network_id"] += str(e)
        
        r["network_value"] = ""
        
        ret.append(r)

    return ret

@lru_cache()
def getnwadr(ipa):
    return getipinfo(ipa, lambda x: [x.nwadr, x.bitmask])

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
    
    for gid, row in groupby(rows, itemgetter("group_id")):
        groups.append(
            Group(id=gid, stmts=[Attr(name='label', value=gid),
                Attr(name='fontsize', value="16"),
                Attr(name='textcolor', value="#FF0000"),
                *[Node(id=r["node_id"], attrs=[]) for r in row],
            ]))
    
    if groups:
        return Diagram(id=['nwdiag', None], stmts=[*groups, *networks])
    else:
        return Diagram(id=['nwdiag', None], stmts=networks)


#def parse(rows):
#    rows = cleansing(rows)


#    networks = {}

#    for r in rows:
#        # make dictionary
#        key = (r["network_id"], r["network_value"])
#        val = (r["node_id"], r["node_value"], r["group_id"])
#        if key in networks:
#            networks[ key ].append( val )
#        else:
#            networks[ key ] = [ val ]

#    # create Diagram Object Data
#    na = networks.copy().keys()
#    nb = dict(na)
#    if len(na) > len(nb):
#        for k, v in na:
#            if k in nb:
#                new = ("{}\n({})".format(k, v) if k else v, None)
#                networks[new] = networks.pop((k, v))

#    # finalize make data
#    ret = []
#    g = {}

#    for (nwid, nwadr), nodes in networks.items():

#        # many ip addresses
#        n = {}

#        for node_id, ip, gid in nodes:
#            if node_id in n:
#                n[node_id].append(ip)
#            else:
#                n[node_id] = [ip]

#            if gid and gid.strip():
#                if gid in g:
#                    g[gid].append(node_id)
#                else:
#                    g[gid] = [node_id]


#        ret.append(
#            Network(id=nwid, stmts=[
#                Attr(name='address', value=nwadr),
#                    *[Node(id=nid, attrs=[Attr(name='address', value=", ".join(ips))]) for nid, ips in n.items()]
#            ])
#        )

#    if g:
#        return Diagram(id=['nwdiag', None], stmts=[
#            Group(id=gi, stmts=[
#                Attr(name='label', value=gi),
#                Attr(name='fontsize', value="16"),
#                Attr(name='textcolor', value="#FF0000"),
#                    *[Node(id=nd, attrs=[]) for nd in ni]]) for gi, ni in g.items()
#                ] + ret)

#    return Diagram(id=['nwdiag', None], stmts=ret)


def render(rows, outpath=None, outtype="SVG", autoopen=True):
    diag = parse(rows)

    if diag.stmts:
        app = NwdiagApp()
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
 ['nandeyanen', 'seg', 'server4', '', '10.111.111.3', '', '']]

    diag = parse(rows)
    test1(diag)

def test1(diag):
    import time
    now = time.time()
    app = NwdiagApp()
    app.options.type = "PNG"
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

