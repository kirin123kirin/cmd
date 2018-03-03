import pandas as pd
import plotly.offline as py
import random
import re
from collections import Counter

rc = lambda: random.randint(0, 255)
def randomrgb(L, transparent=0.8):
    return ['rgba({}, {}, {}, {})'.format(rc(), rc(), rc(),transparent) for i in L]

def render_sankey(df, title="", width = None, height = None,
        orientation = "h", valueformat = ".0f", valuesuffix = ""):

    nodes = pd.concat([df.source, df.target]).unique().tolist()

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
          pad = 20, #ノードの間隔
          thickness = 20, #ノードの太さ
          line = dict(
            color = "black",
            width = 0.5
          ),
          label =  nodes,
          color =  randomrgb(nodes),

        ),
        link = dict(
          source =  [nodes.index(x) for x in df.source],
          target =  [nodes.index(x) for x in df.target],
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
    from pandas.io.clipboard import clipboard_get
    
    #render_sankey(pd.read_clipboard())
    tsvsankey(clipboard_get())

if __name__ == "__main__":
    main()
