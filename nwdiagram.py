#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import os
import codecs
import sys
import traceback
from tempfile import gettempdir
import subprocess
from pathlib import Path

from blockdiag.command import BlockdiagApp
from seqdiag.command import SeqdiagApp
from actdiag.command import ActdiagApp
from nwdiag.command import NwdiagApp
from rackdiag.command import RackdiagApp
from packetdiag.command import PacketdiagApp


iswin = os.name == "nt"



def render(func, diag, outtype="svg"):
    p = Path(sys.argv[0])
    f = os.path.join(gettempdir(), p.name + ".diag")
    with codecs.open(f, "w", encoding="utf-8") as tmp:
        tmp.write(diag)
        out = "{}.{}".format(Path(f).stem, outtype)

    try:
        if os.path.exists(out):
            os.remove(out)
        func().run([f, "-T", outtype])
    except:
        traceback.print_exc()
    else:
        if iswin:
            cmd = "start "
        else:
            cmd = "open "
        subprocess.check_call(cmd + out, shell=True)
    finally:
        os.remove(f)


def nwdiag(df, nw_name="ネットワーク名", nw_addr="ネットワークアドレス", host_name="ホスト名", ip_addr="IPアドレス", outtype="svg"):
    tp = """nwdiag {{
        {}
    }}
    """
    
    nw = """
      network {} {{
          address = "{}";
          {}
      }}
    """
    
    ht = """
          {} [address = "{}"];
    """
    
    nws = ""
    for (nwnm, nwad), gdf in df.groupby([nw_name, nw_addr]):
        host = ""
        for hostnm, ipdf in gdf.groupby([host_name]):
            adr = ", ".join(ipdf[ip_addr].tolist()) 
            host += ht.format(hostnm, adr)
        nws += nw.format(nwnm, nwad, host)
    diag = tp.format(nws)

    render(NwdiagApp, diag, outtype)

def blockdiag():
    pass

#blockdiag admin {
#  index [label = "List of FOOs"];
#  add [label = "Add FOO"];
#  add_confirm [label = "Add FOO (confirm)"];
#  edit [label = "Edit FOO"];
#  edit_confirm [label = "Edit FOO (confirm)"];
#  show [label = "Show FOO"];
#  delete_confirm [label = "Delete FOO (confirm)"];
#
#  index -> add  -> add_confirm  -> index;
#  index -> edit -> edit_confirm -> index;
#  index -> show -> index;
#  index -> delete_confirm -> index;
#}



def seqdiag():
    pass

def actdiag():
    pass

def rackdiag():
    pass

def packetdiag():
    pass




if __name__ == "__main__":
    outtype = "svg"
    df = pd.read_clipboard(dtype="object", engine="python")
    nwdiag(df, nw_name="ネットワーク名", nw_addr="ネットワークアドレス", host_name="ホスト名", ip_addr="IPアドレス", outtype=outtype)




