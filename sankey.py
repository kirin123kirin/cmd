#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__  = 'm.yama'
__license__ = 'MIT'
__version__ = '0.1.1'


__all__     = [
    "render",
]

import os
import sys
import json
from lzma import LZMAFile
from tempfile import gettempdir
from subprocess import getstatusoutput
from hashlib import md5

import util
from util.core import flatten
from util.io import readrow

join, dirname = os.path.join, os.path.dirname


tmpl = """<html><head><meta charset="utf-8" /></head>
<body>
<div><script type="text/javascript">window.PlotlyConfig = {{MathJaxConfig: 'local'}};</script>
<script type="text/javascript" src="sankey.js"></script>
<div id="d20ac8fc-6cf3-4c8d-bc7e-ab4d283ae987" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
window.PLOTLYENV=window.PLOTLYENV || {{}};
window.PLOTLYENV.BASE_URL='http://type-here.com';
if (document.getElementById("d20ac8fc-6cf3-4c8d-bc7e-ab4d283ae987")) {{
    Plotly.newPlot('d20ac8fc-6cf3-4c8d-bc7e-ab4d283ae987',
        {trace},{layout},{{"showLink": false,"linkText": "Export to type-here.com","responsive": true,"plotlyServerURL": "http://type-here.com"}}
    )
}};
</script></div></body></html>
"""


def buildhtml(
    trace,
    layout,
    template = join(dirname(util.__file__),"libs/sankey.js.xz"),
    outpath = join(gettempdir(), "temp-plot.html"),
    updatejs = False
    ):

    js = join(dirname(outpath),"sankey.js")
    if updatejs or not os.path.exists(js):
        with LZMAFile(template, 'rb') as fp, open(js, "wb") as wp:
            wp.write(fp.read())

    html = tmpl.format(
        trace=json.dumps(trace),
        layout=json.dumps(layout)
    )

    with open(outpath, "w") as fp:
        fp.write(html)

    if os.name == "nt":
        code, dat = getstatusoutput("start " + outpath)
        if code != 0:
            raise RuntimeError(dat)
    else:
        print("output: " + outpath)


def getsankeydict(
    nodes,
    colors,
    links_source, links_target,
    links_value, links_label,
    title="", width = None, height = None,
    orientation = "h", valueformat = ".0f", valuesuffix = ""
    ):

    trace = dict(
        type='sankey',
        width = width,
        height = height,
        domain = dict(
          x =  [0,1],
          y =  [0,1]
        ),
        orientation = orientation,
        valueformat = valueformat,
        valuesuffix = valuesuffix,
        node = dict(
          pad = 40, #ノードの間隔
          thickness = 20, #ノードの太さ
          line = dict(
            color = "black",
            width = 0.5
          ),
          label =  nodes,
          color =  colors,#randomrgb(nd),

        ),
        link = dict(
          source =  links_source,
          target =  links_target,
          value =  links_value,
          label =  links_label
      ))

    layout =  dict(
        title = title, # HTMLタグが使える
        font = dict(
          size = 10
        )
    )

    return dict(trace=[trace], layout=layout)


def colorcode(a):
    if not a:
        return "green"
    return "#" + md5(a.encode()).hexdigest()[:6].upper()


def sankey_flatten(
    table,
    title="",
    width = None,
    height = None,
    orientation = "h",
    valueformat = ".0f",
    valuesuffix = ""
    ):

    nodes = sorted(x for x in set(flatten(table)) if x)
    colors = ["green"] * len(nodes)
    links_h = [lnk for x in table for lnk in zip(x, x[1:]) if all(lnk)]
    links = list(zip(*links_h))

    return getsankeydict(
        nodes=nodes,
        colors=colors,
        links_source = list(map(nodes.index, links[0])),
        links_target = list(map(nodes.index, links[1])),
        links_label = ["{} -> {}".format(a, b) for a, b in links_h],
        links_value = [10] * len(links_h),
    )


def sankey_table(
    table,
    title="",
    width = None,
    height = None,
    orientation = "h",
    valueformat = ".0f",
    valuesuffix = ""
    ):

    head, table = table[0], table[1:]

    nd = {}
    links = {}

    for t in table:
        s = dict(zip(head, t))
        try:
            src = s["from"]
            nd[src] = colorcode(s["fromgroup"] if "fromgroup" in s else src)
        except KeyError:
            pass

        try:
            tar = s["to"]
            nd[tar] = colorcode(s["togroup"] if "togroup" in s else tar)
        except KeyError:
            pass

        if (src, tar) in links:
            links[(src, tar)] += 10
        else:
            links[(src, tar)] = 10

    nodes = list(nd.keys())
    return getsankeydict(
        nodes = nodes,
        colors = list(nd.values()),
        links_source = [nodes.index(k) for k, v in links],
        links_target = [nodes.index(v) for k, v in links],
        links_value = list(links.values()),
        links_label = ["{} -> {}".format(*x) for x in links],
    )


def render(
    path_or_buffer=None,
    outpath = join(gettempdir(), "temp-plot.html"),
    title="",
    width = None,
    height = None,
    orientation = "h",
    valueformat = ".0f",
    valuesuffix = "",
    updatejs=False,
    ):

    if path_or_buffer:
        reader = readrow.csv(path_or_buffer)
    else:
        reader = readrow.clipboard()
    table = [[x for x in r.value if x] for r in reader]

    header = ["from", "to", "fromgroup", "togroup", "value"]
    chk = len(set(header) & set(table[0]))
    
    if chk >= 4:
        snkdic = sankey_table(table)
    elif chk >= 2:
        raise ValueError("Unknown header Values (valid is `{}`)".format(", ".join(header)))
    else:
        snkdic = sankey_flatten(table)

    buildhtml(**snkdic, outpath=outpath, updatejs=updatejs)


def main():
    from argparse import ArgumentParser

    ps = ArgumentParser(prog="sankey",
                        description="sankey diagram build program\n")
    padd = ps.add_argument

    padd("file",
         metavar="<file>",
         nargs="?",  default=None,
         help="sankey diagram source csv, tsv filepath\ndefault clipboard")

    padd('-t', '--title', type=str, default="",
         help='output HTML title string')
    padd('-o', '--outpath', type=str, default=join(gettempdir(), "temp-plot.html"),
         help='output file PATH string `default $TMP/temp-plot.html`')
    padd('-W', '--width', type=int, default=None,
         help='output HTML width pixel')
    padd('-H', '--height', type=int, default=None,
         help='output HTML height pixel')
    padd('-U', '--updatejs', action="store_true", default=False,
         help='update javascript sankey.js')

    args = ps.parse_args()

    render(
        path_or_buffer = args.file,
        outpath = args.outpath,
        title = args.title,
        width = args.width,
        height = args.height,
        updatejs = args.updatejs
    )


if __name__ == "__main__":
    main()




