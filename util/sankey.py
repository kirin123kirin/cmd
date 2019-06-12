#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__  = 'm.yama'
__license__ = 'MIT'
__version__ = '0.0.2'


__all__     = [
    "render_sankey",
    "tsvsankey",
]

import pandas as pd
import plotly.offline as py
import re
from collections import Counter
from hashlib import md5

def colorcode(a):
    if not a:
        return "green"
    return "#" + md5(a.encode()).hexdigest()[:6].upper()

def render_sankey(df, title="", width = None, height = None,
        orientation = "h", valueformat = ".0f", valuesuffix = ""):

    if "source_group" not in df.columns:
        df["source_group"] = ""

    if "target_group" not in df.columns:
        df["target_group"] = ""


    df.sort_values(["source_group", "target_group"], inplace=True)
    if "value" in df.columns:
        df["value"] = df.value.astype(int)
    else:
        df["value"] = 1
        df[["source","target","value"]] = df.groupby(["source", "target"], as_index=False).sum()

    if "label" not in df.columns:
        df["label"] = df.source.fillna("") + " -> " + df.target.fillna("")

    nc = ["label", "group"]
    df = df[df["source"] != df["target"]]

    src = df[["source", "source_group"]]
    tar = df[["target","target_group"]]
    src.columns = nc
    tar.columns = nc

    nodes = pd.concat([src, tar])
    nodes.drop_duplicates(inplace=True)
    nodes.dropna(inplace=True)
    nodes.reset_index(inplace=True, drop=True)
    nd = nodes.label.tolist()

    nodes["color"] = nodes.group.fillna("").astype(str).apply(colorcode)

    data_trace = dict(
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
          label =  nd,
          color =  nodes["color"].tolist(),#randomrgb(nd),

        ),
        link = dict(
          source =  [nd.index(x) for x in df.source],
          target =  [nd.index(x) for x in df.target],
          value =  df.value.tolist(),
          label =  df.label.tolist()
      ))

    layout =  dict(
        title = title, # HTMLタグが使える
        font = dict(
          size = 10
        )
    )

    fig = dict(data=[data_trace], layout=layout)

    py.plot(fig, validate=False)

def tsvsankey(txt, sep="\t"):
    sep = re.compile(sep + "+")
    strip = re.compile(sep.pattern + "$")

    ret=Counter()

    for t in txt.splitlines():
        m = sep.split(strip.sub("",t))
        if len(m) == 1:
            ret[(m[0],"")] += 1
            continue
        for s in range(len(m)):
            try:
                ret[(m[s], m[s+1])] += 1
            except IndexError:
                pass

    render_sankey(pd.DataFrame({"source":k[0], "target":k[1], "value":v, "label":""} for k,v in ret.items()))

def main():

    try:
        render_sankey(pd.read_clipboard(engine="python", dtype="object", keep_default_na=False))
    except:
        from pandas.io.clipboard import clipboard_get
        tsvsankey(clipboard_get())

if __name__ == "__main__":
    main()
