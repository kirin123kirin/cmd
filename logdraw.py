# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pandas.tseries.offsets as offsets
from matplotlib.font_manager import FontProperties
import re

fp = FontProperties(fname=r'C:\WINDOWS\Fonts\meiryo.ttc', size=8)
WK  = "月,火,水,木,金,土,日".split(",")

__renderdoc = """ seabornライブラリを利用してグラフを書きます
    df          : pandasのデータフレーム
    div_label   : グラフを分割するキーとなるラベル名 -> dfのカラム名を渡せ str型
    x_label     : 各子グラフのX軸としたいラベル名   -> dfのカラム名を渡せ str型
    y_label     : 各子グラフのY軸としたいラベル名   -> dfのカラム名を渡せ str型
    wrap        : 横に何個ずつグラフを分割したいか　 -> int型
    title       : グラフのタイトルをつけたい時入れる
    color       : 色を指定 -> blue, red, syan ...etc
                  (参考 https://pythondatascience.plavox.info/matplotlib/%E8%89%B2%E3%81%AE%E5%90%8D%E5%89%8D)
    func        : グラフ描画する関数を渡せ
    *arg, **kw  : seaborn.FacetGridの引数に委譲します
    """

def sns_render(g, x_label, y_label, title="", color="blue", func=sns.boxplot):
    g.set_xticklabels(rotation=90)
    g.set_titles(fontproperties=fp)
    g.map(func, x_label, y_label,color=color)
    plt.subplots_adjust(top=0.92)
    if title:
        g.fig.suptitle(title,fontproperties=fp)

def render_horizon(df, div_label, x_label, y_label,
                   wrap=2, title="", color="blue", func=sns.boxplot, *arg, **kw):
    g = sns.FacetGrid(df, col=div_label,col_wrap=wrap, ylim=(0,100), *arg, **kw)
    sns_render(g, x_label, y_label, title, color, func)

def render_vertical(df, div_label, x_label, y_label,
                    wrap=2, title="", color="blue", func=sns.boxplot, *arg, **kw):
    g = sns.FacetGrid(df, row=div_label, ylim=(0,100), *arg, **kw)
    sns_render(g, x_label, y_label, title, color, func)

render_horizon.__doc__ = __renderdoc
render_vertical.__doc__ = __renderdoc

def render_heatmap(df, index, columns, values, types="avg", color="Reds", *arg, **kw):
    r = df[[index,columns,values]].groupby([index, columns], as_index=False)
    tp = types.lower()
    if tp == "avg":
        ret = r.mean()
    elif tp == "min":
        ret = r.min()
    elif tp == "max":
        ret = r.max()
    elif "tile" in tp:
        n = float(re.findall("\d+", types)[0])
        ret = r.quantile(n / 100)

    sns.heatmap(ret.pivot(index,columns,values), cmap=color, vmax=100, *arg, **kw)

## IT統合基盤用リソースCSVのパース用
def floor_minutes(time, step):
    h, m = map(int, time.split(":"))
    return "{:02d}:{:02d}".format(h, (int)(m / step) * step)


def parse(fn):
    print("INFO: {}を読み込み中... ".format(fn))
    ret = pd.read_csv(fn,
                    parse_dates={'datetime':['DATE','TIME']},
                    usecols=["DATE","TIME","Used(avg)"],
                    engine="python"
                    )
    gd  = ret.datetime - offsets.Hour(22)
    ret.loc[:, "gyoumudate"] = gd.dt.strftime("%m/%d")
    ret.loc[:, "weekday"]    = gd.dt.weekday.apply(lambda x: WK[x])
    del gd
    ret.loc[:, "time"]       = ret.datetime.dt.strftime("%H:%M")
    ret.loc[:, "week"]       = ret.datetime.dt.weekofyear
    ret.set_index("datetime",inplace=True)
    return ret

if __name__ == "__main__":
    import optparse as ops
    from glob import glob

    # DEBUG用
    sys.argv.extend([r"C:\Users\yellow\Documents\resource\cpu_20171101-20171205.csv"])

    usage = "usage: %prog [-y] [-w] [-i n] ファイル名..."
    op = ops.OptionParser(usage)
    op.add_option("-n", "--nobatchtime", action="store_false", default=True,
                            help="バッチ時間22-9時までに絞らない デフォルトは絞ります")
    op.add_option("-y", "--weekdayly", action="store_true", default=False,
                            help="曜日別サマリを表示する")
    op.add_option("-w", "--weekly", action="store_true", default=False,
                            help="週別サマリを表示する")
    op.add_option("--heatmap", action="store_true", default=False,
                            help="日別＆時刻別ヒートマップを表示する")
    op.add_option("-i", "--interval",action="store", type="int", default=10,
                            help="何分間隔にグラフ描画するか指定するデフォルトは10分間隔")
    op.add_option("-s", "--startdatetime",action="store", type="str", default=None,
                            help="開始日時条件があれば入れる 結構日時フォーマットは柔軟に対応!?")
    op.add_option("-e", "--enddatetime",action="store", type="str", default=None,
                            help="終了日時条件があれば入れる 結構日時フォーマットは柔軟に対応!?")
    options, args = op.parse_args()

    args = sum(list(map(glob, args)), []) # <- flatten

    start = options.startdatetime
    end  = options.enddatetime

    if len(args) > 1:
        df = pd.concat(parse(fn).loc[start : end] for fn in args)
    elif len(args) == 1:
        df = parse(args[0]).loc[start : end]
    else:
        sys.stderr.write("ターゲットファイルが一つも見つからず空振りました。\n\n" + op.usage)
        exit(1)

    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    df["Used(avg)"].plot(kind="area", ylim=(0,100))

    if options.nobatchtime:
        dim = [22,23,0,1,2,3,4,5,6,7,8]
        df = df[df.index.hour.isin(dim)]
    gt = df["time"].drop_duplicates()
    gt.index = (gt.str.slice(0,2).astype(int).apply(dim.index) * 100) + gt.str.slice(3,5).astype(int)
    gt.sort_index(inplace=True)

    if options.weekdayly or options.weekly or options.heatmap:
        df.loc[:, "time"]  = df.time.apply(floor_minutes, args=(options.interval,))
        func = sns.violinplot #barplot, boxplot, point, violinplot # 使えたメソッド達

    if options.weekdayly:
        render_horizon(df, "weekday", "time", "Used(avg)", title="曜日別サマリ",
                       func=func, col_order=WK)

    if options.weekly:
        render_horizon(df, "week", "time", "Used(avg)",title="週別サマリ",
                       func=func)

    if options.heatmap:
        render_heatmap(df, "gyoumudate","time","Used(avg)", types="avg", xticklabels=gt.values)

    plt.show()

