# -*- coding: utf-8 -*-
import pandas as pd
import difflib
from profiler import pandas_read_any, logger, outputter
import os,sys
import psutil as ps
enc        = "cp932"

class DiffCode:
    SIMILAR = "Same"         # starts with '  '
    RIGHTONLY = "!Append"       # starts with '+ '
    LEFTONLY = "!Delete"        # starts with '- '
    CHANGED = "!Change"         # either three or four lines with the prefixes ('-', '+', '?'), ('-', '?', '+') or ('-', '?', '+', '?') respectively

class DifflibParser:
    def __init__(self, text1, text2):
        self.__text1 = text1
        self.__text2 = text2
        self.__diff = list(difflib.ndiff(text1, text2))
        self.__currentLineno = 0

    def __iter__(self):
        return self

    def __next__(self):
        result = {}
        if self.__currentLineno >= len(self.__diff):
            raise StopIteration
        currentLine = self.__diff[self.__currentLineno]
        code = currentLine[:2]
        line = currentLine[2:]
        if code == '  ':
            result['before'] = line
            result['code'] = DiffCode.SIMILAR
        elif code == '- ':
            result['before'] = line
            incrementalChange = self.__tryGetIncrementalChange(self.__currentLineno)
            if not incrementalChange:
                result['code'] = DiffCode.LEFTONLY
            else:
                result['code'] = DiffCode.CHANGED
                # result['leftchanges'] = incrementalChange['left'] if 'left' in incrementalChange else None
                # result['rightchanges'] = incrementalChange['right'] if 'right' in incrementalChange else None
                result['after'] = incrementalChange['after']
                self.__currentLineno += incrementalChange['skiplines']
        elif code == '+ ':
            result['after'] = line
            result['code'] = DiffCode.RIGHTONLY
        self.__currentLineno += 1
        return result

    def __tryGetIncrementalChange(self, lineno):
        lineOne = self.__diff[lineno] if lineno < len(self.__diff) else None
        lineTwo = self.__diff[lineno + 1] if lineno + 1 < len(self.__diff) else None
        lineThree = self.__diff[lineno + 2] if lineno + 2 < len(self.__diff) else None
        lineFour = self.__diff[lineno + 3] if lineno + 3 < len(self.__diff) else None

        changes = {}
        # ('-', '?', '+', '?') case
        if lineOne and lineOne[:2] == '- ' and \
           lineTwo and lineTwo[:2] == '? ' and \
           lineThree and lineThree[:2] == '+ ' and \
           lineFour and lineFour[:2] == '? ':
            # changes['left'] = [i for (i,c) in enumerate(lineTwo[2:]) if c in ['-', '^']]
            # changes['right'] = [i for (i,c) in enumerate(lineFour[2:]) if c in ['+', '^']]
            changes['after'] = lineThree[2:]
            changes['skiplines'] = 3
            return changes
        # ('-', '+', '?')
        elif lineOne and lineOne[:2] == '- ' and \
           lineTwo and lineTwo[:2] == '+ ' and \
           lineThree and lineThree[:2] == '? ':
            # changes['right'] = [i for (i,c) in enumerate(lineThree[2:]) if c in ['+', '^']]
            # changes['left'] = []
            changes['after'] = lineTwo[2:]
            changes['skiplines'] = 2
            return changes
        # ('-', '?', '+')
        elif lineOne and lineOne[:2] == '- ' and \
           lineTwo and lineTwo[:2] == '? ' and \
           lineThree and lineThree[:2] == '+ ':
            # changes['right'] = []
            # changes['left'] = [i for (i,c) in enumerate(lineTwo[2:]) if c in ['-', '^']]
            changes['after'] = lineThree[2:]
            changes['skiplines'] = 2
            return changes
        # no incremental change
        else:
            return None

def _differ(before_df, after_df):
    diff = DifflibParser(before_df.to_csv(sep="\t",header=False,index=False).splitlines(),
                                          after_df.to_csv(sep="\t",header=False,index=False).splitlines())
    t_bdf = before_df.T
    t_adf = after_df.T
    for d in diff:
        if d["code"].startswith("!"):
            if d.get("before"):
                bsr = pd.Series(d["before"].split("\t"), index=before_df.columns)
                bL = (before_df.loc[t_bdf.isin(bsr).all()].index + 1).tolist()
            if d.get("after"):
                asr = pd.Series(d["after"].split("\t"), index=after_df.columns)
                aL = (after_df.loc[t_adf.isin(asr).all()].index + 1).tolist()
            if d["code"] == DiffCode.CHANGED:
                babool = bsr != asr
                chstr = bsr[babool].astype(str) + "===>" + asr[babool].astype(str)
                bsr[chstr.index] = chstr
                bsr["LineNo"] = "{}<===>{}".format(bL[0],aL[0])
                yield bsr
            elif d["code"] == DiffCode.LEFTONLY: #Delete
                bsr["LineNo"] = "{}<===DEL".format(bL[0])
                yield bsr
            elif d["code"] == DiffCode.RIGHTONLY: #Append
                asr["LineNo"] = "ADD===>{}".format(aL[0])
                yield asr
    del t_bdf, t_adf, diff

def pandas_df_diff(before_df, after_df, keys=[], changed_only=True):
    if keys:
        s = keys + list(filter(lambda x: x not in keys, before_df.columns))
    else:
        s = before_df.columns.tolist()
    outcolumn = before_df.columns.tolist() + ["LineNo"]
    c = pd.DataFrame(_differ(before_df[s], after_df[s]))[outcolumn]
    if changed_only:
        col = c[c.apply(lambda x: x.str.match(".*===.*"))].dropna(axis=1, how="all").columns
        return c[col]
    else:
        return c

def main(f_before, f_after,keys=[], changed_only=True, header=True, enc="cp932"):
    filesize = "beforefile {:,.1f}KB , afterfile {:,.1f}KB".format(os.stat(f_before).st_size / 1024,os.stat(f_after).st_size / 1024)
    logger("[DEBUG] loading size %s)" % (filesize))
    
    ret = pandas_df_diff(pandas_read_any(f_before,encoding=enc),
                                          pandas_read_any(f_after,encoding=enc),
                                          keys=keys,
                                          changed_only=changed_only)
    outputter(ret.to_csv(index=False, header= header, encoding=enc))

if __name__ == '__main__':
    import optparse
    enc        = "cp932"
    verbose    = False
    
    usage = "usage: differ [-v] [-c] [-e utf-8] [-k col1,col2..] ??????? ??????? > diff_out.csv\n"
    op = optparse.OptionParser(usage)

    op.add_option("-v", "--verbose", action="store_true", default=False,
                            help="????")
    op.add_option("-c", "--changed_only",action="store_true", default=False,
                            help="???????")
    op.add_option("-e", "--encoding", type='choice', choices=['cp932', 'utf-8', 'eucjp'], default="cp932",
                            help="???????(?????cp932)\n???cp932 utf-8 eucjp")
    op.add_option("-k", "--keys", type='string', default=[],
                            help="??????\n??????????????")

    options, args = op.parse_args()

    if len(args) != 2:
        sys.stderr.write("????????????????????\n\n" + op.usage)
        exit(1)

    enc         = options.encoding
    verbose = options.verbose
    changed_only     = options.changed_only
    header   = True
    keys = options.keys.split(",")

    
    main(args[0], args[1], keys, changed_only, header, enc)
        



