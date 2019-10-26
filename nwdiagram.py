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
        font=[],
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


def _cleanse_header(header):
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

    cleaned = [to_hankaku(x).lower().replace(" ", "").replace("名", "") for x in header]
    return [_cleansing.get(h, h) for h in cleaned]

def parse(rows):
    header = _cleanse_header(next(rows).value)

    networks = {}

    for x in rows:
        dat = dict(zip(header, x.value))
        get = dat.get

        group_id = get("group_id", "")
        nwname = get("network_id", "")
        server = get("_server_node_id")
        host = get("_host_node_id")
        ip = get("_ip")
        prefix = get("_prefix")
        subnet = get("_subnet")

        # server hostname normalize
        if server and host:
            node_id = "{}\n({})".format(server, host)
        elif server and not host:
            node_id = server
        elif not server and host:
            node_id = host
        else:
            node_id = None

        # calculate NW address

        if prefix and subnet:
            ip_prefix = ip + "/" + prefix
        elif prefix and not subnet:
            ip_prefix = ip + "/" + prefix
        elif not prefix and subnet:
            ip_prefix = ip + "/" + subnet
        else:
            ip_prefix = ip

        # calculate NW address
        nwip = getipinfo(ip_prefix , lambda x: "{}/{}".format(x.nwadr, x.bitmask))
        if subnet:
            nwip += "\n({})".format(subnet)

        # make dictionary
        key = (nwname, nwip)
        if key in networks:
            networks[ key ].append( (node_id, ip, group_id) )
        else:
            networks[ key ] = [ (node_id, ip, group_id) ]

    # create Diagram Object Data
    na = networks.copy().keys()
    nb = dict(na)
    if len(na) > len(nb):
        for k, v in na:
            if k in nb:
                new = ("{}\n({})".format(k, v) if k else v, None)
                networks[new] = networks.pop((k, v))

    # finalize make data
    ret = []
    g = {}

    for (nwid, nwadr), nodes in networks.items():

        # many ip addresses
        n = {}

        for node_id, ip, gid in nodes:
            if node_id in n:
                n[node_id].append(ip)
            else:
                n[node_id] = [ip]

            if gid and gid.strip():
                if gid in g:
                    g[gid].append(node_id)
                else:
                    g[gid] = [node_id]


        ret.append(
            Network(id=nwid, stmts=[
                Attr(name='address', value=nwadr),
                    *[Node(id=nid, attrs=[Attr(name='address', value=", ".join(ips))]) for nid, ips in n.items()]
            ])
        )

    if g:
        return [
            Group(id=gi, stmts=[
                Attr(name='label', value=gi),
                Attr(name='fontsize', value="16"),
                Attr(name='textcolor', value="#FF0000"),
                    *[Node(id=nd, attrs=[]) for nd in ni]]) for gi, ni in g.items()
                ] + ret

    return ret


def render(rows, outpath=None, outtype="SVG", autoopen=True):
    diag = Diagram(id=['nwdiag', None], stmts=parse(rows))

    if diag.stmts:
        NwdiagApp._options.type = outtype

        app = NwdiagApp()
        app.writediag(diag, outpath=outpath)

    else:
        raise ValueError("empty input data?")

    if autoopen:
        if os.name == "nt":
            cmd = "start "
        else:
            cmd = "open "

        subprocess.check_call(cmd + app.options.output, shell=True)

def test(diag):
    app = NwdiagApp()
    app.writediag(diag, outpath=None)
    subprocess.check_call("start " + app.options.output, shell=True)

def main():
    rows = readrow.clipboard("csv")
    render(rows)

if __name__ == "__main__":
    main()
#    diag = Diagram(id=['nwdiag', None], stmts=[Network(id='Sample_front', stmts=[Attr(name='address', value='"192.168.10.0/24"'), Group(id='web', stmts=[Node(id='web01', attrs=[Attr(name='address', value='".1"')]), Node(id='web02', attrs=[Attr(name='address', value='".2"')])])]), Network(id='Sample_back', stmts=[Attr(name='address', value='"192.168.20.0/24"'), Node(id='web01', attrs=[Attr(name='address', value='".1"')]), Node(id='web02', attrs=[Attr(name='address', value='".2"')]), Node(id='db01', attrs=[Attr(name='address', value='".101"')]), Node(id='db02', attrs=[Attr(name='address', value='".102"')]), Group(id='db', stmts=[Attr(name='label', value='hoge'), Node(id='db01', attrs=[]), Node(id='db02', attrs=[])])])])
#    test(diag)