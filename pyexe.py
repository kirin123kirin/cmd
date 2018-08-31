#!/usr/bin/env python3
from os import remove
from os.path import abspath, normpath, splitext, basename, join as pathjoin
import sys
import codecs
from tempfile import gettempdir
from subprocess import check_call


CSHARP_TEPLATE = """using System;
using System.Text;
using System.IO;
using System.Diagnostics;

class Program
{{
  static StringBuilder output = new StringBuilder();
  static StringBuilder error = new StringBuilder();
  
  static void Main(string[] args)
  {{
    string src = @"{py_src}";
    string temppy = Path.GetTempPath() + "\\\\" + "{prog}";
    Encoding Enc = Encoding.GetEncoding("UTF-8");
    StreamWriter writer =
      new StreamWriter(temppy, true, Enc);
    writer.WriteLine(src);
    writer.Close();

    Process p = new Process();
    p.StartInfo.FileName = "python.exe";
    p.StartInfo.Arguments =  " " + temppy + " " + String.Join(" ", args);
    p.StartInfo.CreateNoWindow = true;
    p.StartInfo.UseShellExecute = false;
    p.StartInfo.RedirectStandardOutput = true;
    p.StartInfo.RedirectStandardInput = true;
    p.StartInfo.RedirectStandardError = true;
    p.OutputDataReceived += OutputHandler;
    p.ErrorDataReceived += ErrorHandler;

    p.Start();
    p.BeginOutputReadLine();
    p.BeginErrorReadLine();

    p.WaitForExit();
    p.Dispose();

    Console.Write(output.ToString());
    Console.Error.Write(error.ToString());
    
    File.Delete(temppy);
    System.Environment.Exit(0);
  }}

  static void OutputHandler(object o, DataReceivedEventArgs args) {{
    output.AppendLine(args.Data);
  }}

  static void ErrorHandler(object o, DataReceivedEventArgs args) {{
    error.AppendLine(args.Data);
  }}

}}


"""

def readpy(path):
    return codecs.open(abspath(path), "r", encoding="utf-8").read().replace('"', '""')

#sys.argv.append("C:/temp/build/profiler.py")

bn = basename(sys.argv[1])
tmp = pathjoin(gettempdir(), splitext(bn)[0])

with codecs.open(tmp,"w", encoding="utf_8_sig") as w:
    out = CSHARP_TEPLATE.format(prog=splitext(bn)[0], py_src=readpy(sys.argv[1]))
    w.write(out)
    fname = normpath(w.name)

buildcmd = "csc " + " ".join(sys.argv[2:]) + " " + fname
check_call(buildcmd, shell=True)
remove(tmp)

