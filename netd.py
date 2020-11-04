import os
from os.path import join, dirname
import json
from operator import attrgetter
from tempfile import gettempdir
from subprocess import getstatusoutput
import tarfile

import util
from util.io import readrow
from util.core import to_hankaku

_header_replace = {
    "受信": "接続",
    "送信": "接続",
    "宛先": "接続先",
    " ": "",
    "名": "",
    "ー": "",
}

_data_replace = {
    "→": "->",
    "←": "<-",
    "↔": "<=>",
    "<->": "<=>",
}

_cleansing = {
    "接続元場所": "_nodesourcegroup1",
    "接続元システム": "_nodesourcegroup3",
    "接続元拠点": "_nodesourcegroup1",
    "接続元ビル": "_nodesourcegroup1",
    "接続元デタセンタ": "_nodesourcegroup2",
    "接続元dc": "_nodesourcegroup2",

    "接続元サバ": "source",
    "接続元ホスト": "source",

    "接続先サバ": "target",
    "接続先ホスト": "target",

    "接続元ポト": "_sourceport",
    "接続先ポト": "_targetport",

    "プロトコル": "_linkprotocol",
    "protocol": "_linkprotocol",
    "protcol": "_linkprotocol",

    "種類": "_linkcategory",
    "種別": "_linkcategory",

    "接続先場所": "_nodetargetgroup1",
    "接続先システム": "_nodetargetgroup3",
    "接続先拠点": "_nodetargetgroup1",
    "接続先ビル": "_nodetargetgroup1",
    "接続先デタセンタ": "_nodetargetgroup2",
    "接続先dc": "_nodetargetgroup2",

    "接続元ipアドレス": "_nodesourceip",
    "接続元ip": "_nodesourceip",
    "接続先ipアドレス": "_nodetargetip",
    "接続先ip": "_nodetargetip",

    "方向": "_vector",
}
_cleansing = {to_hankaku(k): v for k, v in _cleansing.items()}

htmltmpl = """
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <style type="text/css">
        .group rect {{ opacity: 0.5; }}

        .node {{ color: #fff; }}
        .group rect {{ cursor: move; }}

        .node rect,
        .group rect {{
          stroke: #fff;
          stroke-width: 1.5px;
        }}

        .node text {{
          font-size: 10px;
//          fill: #fff;
        }}

        .link {{
          stroke: #7a4e4e;
//          stroke-width: 3px;
          stroke-opacity: 1;
        }}

        .path-label {{ font-size: 7px; }}

    </style>
    <script src="./d3v3.min.js"></script>
    <script src="./cola.min.js"></script>
    <script src="./inet-henge.min.js"></script>
  </head>

  <body>
    <div id="diagram"></div>
  </body>

  <script>
    var data = {0};

    var diagram = new Diagram('#diagram', data, {{width: {2}, height: {3}, ticks: 3000}});

    diagram.linkWidth(function (link) {{
      if (!link)
        return 1;  // px
      else if (link.bandwidth === '100G')
        return 10; // px
      else if (link.bandwidth === '10G')
        return 3;  // px
      else if (link.bandwidth === '1G')
        return 1;  // px
    }});

    diagram.init({1});
  </script>
</html>

"""

def _headerclean(x):
    r = to_hankaku(x).lower()
    for a, b in _header_replace.items():
        r = r.replace(a, b)
    return r

def _dataclean(x):
    r = x.strip()
    for a, b in _data_replace.items():
        r = r.replace(a, b)
    return r

def cleansing(rows):
    if hasattr(rows, "__next__"):
        _head = next(rows)
    else:
        _head = rows.pop(0)

    if hasattr(_head, "value"):
        _head = _head.value
        rows = map(attrgetter("value"), rows)

    head = [_cleansing.get(h, h) for h in map(_headerclean, _head)]

    ret = []

    for row in rows:
        r = {h: [] for h in head}
        for k, v in zip(head, map(_dataclean, row)):
            r[k].append(v)

        ret.append({k: "\t".join(v) for k, v in r.items()})

    return ret


def _parsenode(r):
    src = dict(name=None, group="", icon="", meta={}) #TODO icon
    tar = dict(name=None, group="", icon="", meta={})

    for k, v in sorted(r.items()):
        if "source" in k:
            if k == "source":
                src["name"] = v
            elif k.startswith("_node"):
                if "group" in k:
                    src["group"] += v + "\n"
                else:
                    k = k.replace("_nodesource", "").replace("_nodetarget", "")
                    src["meta"][k] = v
        elif "target" in k:
            if k == "target":
                tar["name"] = v
            elif k.startswith("_node"):
                if "group" in k:
                    tar["group"] += v + "\n"
                else:
                    k = k.replace("_nodesource", "").replace("_nodetarget", "")
                    tar["meta"][k] = v

    src["group"] = src["group"].strip()
    tar["group"] = tar["group"].strip()

    return [src, tar]

def _parselink(r):
    ret = dict(source=None, target=None, meta={})
    for k, v in r.items():
        if "_node" in k:
            continue
        if "group" not in k:
            if k in ["source", "target"]:
                ret[k] = v
            elif k.startswith("_link"):
                ret["meta"][k.replace("_link", "")] = v
            elif v:
                c = k.replace("_source", "").replace("_target", "")
                if "_source" in k:
                    cv = {"source": v}
                if "_target" in k:
                    cv = {"target": v}

                if c in ret["meta"]:
                    ret["meta"][c].update(cv)
                else:
                    ret["meta"][c] = cv

    return ret

def dictuniq(L):
    ret = []
    for t in set(map(repr, L)):
        d = eval(t)
        if not d["meta"]:
            del d["meta"]
        ret.append(d)
    return ret


def parse(rows):
    nodes = []
    links = []
    metas = set()

    for r in cleansing(rows):
        r = [{k:v for k, v in r.items() if k[0] in "_st"}]

        if "_vector" in r[0]:
            vec = r[0].pop("_vector")
            if vec[0] == "<":
                tr = [{(k.replace("source", "target") if "source" in k else k.replace("target", "source")): v for k, v in r[0].items()}]
                if vec == "<=>":
                    r.append(tr)
                else:
                    r = tr

        for c in r:
            nodes.extend(_parsenode(c))
            link = _parselink(c)
            links.append(link)

            metas.update([*[y for x in nodes for y in x["meta"]], *list(link["meta"])])

    jstr = json.dumps(dict(nodes=dictuniq(nodes), links=dictuniq(links)), indent=4, ensure_ascii=False)
    return jstr, ", ".join(map(repr, metas))


def render(
    path_or_buffer=None,
    template = join(dirname(util.__file__),"libs/inet-henge.tar.xz"),
    outpath = join(gettempdir(), "temp-inet.html"),
    width = 1200,
    height = 900,
    updatejs = False
    ):

    if path_or_buffer:
        rows = readrow(path_or_buffer)
    else:
        rows = readrow.clipboard("csv")

    wdir = dirname(outpath)
    js = join(wdir, "inet-henge.min.js")
    if updatejs or not os.path.exists(js):
        with tarfile.open(template, 'r:xz') as fp:
            fp.extractall(wdir)

    html = htmltmpl.format(*parse(rows), width, height)

    with open(outpath, "w", encoding="utf-8") as fp:
        fp.write(html)

    if os.name == "nt":
        code, dat = getstatusoutput("start " + outpath)
        if code != 0:
            raise RuntimeError(dat)

    print("output: " + outpath)

def test():
    rows = [
     ["接続元場所", "接続元ラック名", "接続元ホスト", "接続元サーバ名", "接続元IPアドレス","接続元ポート", "方向","接続先場所", "接続先ラック名", "接続先ホスト", "接続先サーバ名", "接続先IPアドレス","接続先ポート", "種別", "帯域", "ポートVlan", "タグVlan", "チャネル", "備考"],
     ["西日本DC", "４F", "HOST2", "Webサーバ", "10.0.0.1", "1024～", "→","東日本DC", "３F", "HOST1", "DBサーバ", "10.0.1.1", "80,443", "HTTP", "1M", "", "", "", ""],
     ["東日本DC", "３F", "HOST1", "DBサーバ", "10.0.1.1", "*", "←","西日本DC", "４F", "HOST2", "Webサーバ", "10.0.0.1", "*", "ICMP", "1M", "", "", "", ""]
    ]

    html = htmltmpl.format(*parse(rows), 1200, 900)
    print(html)

def main():
    from argparse import ArgumentParser

    ps = ArgumentParser(prog="network diagram",
                        description="network diagram build program\n")
    padd = ps.add_argument

    padd("file",
         metavar="<file>",
         nargs="?",  default=None,
         help="network diagram source filepath\ndefault clipboard")
    padd('-o', '--outpath', type=str, default=join(gettempdir(), "temp-inet.html"),
         help='output file PATH string `default $TMP/temp-inet.html`')
    padd('-W', '--width', type=int, default=1200,
         help='output HTML width pixel')
    padd('-H', '--height', type=int, default=900,
         help='output HTML height pixel')
    padd('-U', '--updatejs', action="store_true", default=False,
         help='update javascript inet-henge.min.js')

    args = ps.parse_args()

    render(
        path_or_buffer = args.file,
        outpath = args.outpath,
        width = args.width,
        height = args.height,
        updatejs = args.updatejs
    )


if __name__ == "__main__":
    # test()
    main()


