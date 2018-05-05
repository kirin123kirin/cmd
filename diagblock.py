# -*- coding: utf-8 -*-
pth = os.getenv("PYTHONPATH", "")
font= os.getenv("Windir", r"C:\Windows") + r"\Fonts\msgothic.ttc"
outtype="svg"

name = os.path.basename(sys.argv[0])
apname = os.path.splitext(name)[0]

outfile=os.getenv("Temp", r"C:\Windows\Temp") + "\{}.{}".format(apname,outtype)

if os.path.exists(outfile):
    del os.remove(outfile)

exe = os.path.join(pth, "Scripts", apname +".exe")

cmd = "{} -f {} {} -T{}".format(exe, font, sys.argv[1], outtype)
ret = os.system(cmd)
if ret == 0:
    os.system("start " + outfile)
    exit(0)
else:
    raise RuntimeError("Run Fail !  " + exe)
