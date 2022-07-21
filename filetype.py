#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__  = 'm.yama'
__license__ = 'MIT'
__date__    = 'Wed Jul 31 17:35:47 2019'
__version__ = '0.0.1'

__all__ = [
    "guesstype",
    'is_office',
    'is_tar',
    'is_lha',
    'is_xls',
    'is_doc',
    'is_ppt',
    'is_text',
    'is_bin',
    'is_xml',
    'is_html',
    'is_json',
    'is_csv',
    'is_dml',
]

import os
import re
from io import BytesIO, StringIO
import csv
from chardet import detect

def is_office(b:bytes):
    if b[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
        return True
    if b[:2] == b"PK":
        # is pptx?
        if b[30:49] == b"[Content_Types].xml" or b[30:34] == b"ppt/":
            return True
        if b[30:73] == b"mimetypeapplication/vnd.oasis.opendocument.":
            return True
    if b[:19] in [b"\x00\x01\x00\x00Standard Jet DB\x00", b'\x00\x01\x00\x00Standard ACE DB\x00']:
        return True
    return False

def is_tar(b:bytes):
    return b[257:262] == b"ustar"# and b[262] in [b"\x00", b"\x04"]

def is_lha(b:bytes):
    return b[0] == b"!"[0] and b[2:5] == b"-lh" and b[6] == b"-"[0]

def is_xls(b:bytes):
    # is xls
    if b[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
        s = 2**sum(b[30:32]) * sum(b[48:50]) + 640
        mg = b[s:s+16][::2]
        return mg == b"Workbook" or mg[:4] == b"Book"
    # is xlsx
    if b[:2] == b"PK":
        if b[30:49] == b"[Content_Types].xml":
            return b"\x00xl/" in b
        if b[30:].startswith(b"mimetypeapplication/vnd.oasis.opendocument.spreadsheet"):
            return True
    return False

def is_doc(b:bytes):
    # is doc
    if b[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
        return b[512:514] == b"\xec\xa5"
    # is docx
    if b[:2] == b"PK":
        if b[30:49] == b"[Content_Types].xml":
            return b"\x00word/" in b
        if b[30:].startswith(b"mimetypeapplication/vnd.oasis.opendocument.text"):
            return True
    return False

def is_ppt(b:bytes):
    # not xls and not word ==> ppt
    if b[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
        if b[512:514] == b"\xec\xa5":
            return False
        s = 2**sum(b[30:32]) * sum(b[48:50]) + 640
        mg = b[s:s+16][::2]
        if mg == b"Workbook" or mg[:4] == b"Book":
            return False
        if mg:
            return True
    if b[:2] == b"PK":
        # is pptx?
        if b[30:49] == b"[Content_Types].xml" or b[30:34] == b"ppt/":
            return b"\x00ppt/" in b
        if b[30:].startswith(b"mimetypeapplication/vnd.oasis.opendocument.presentation"):
            return True
    return False

def is_text(b:bytes):
    return b"\x00" not in b

def is_bin(b:bytes):
    return b"\x00" in b

def is_xml(b:bytes):
    return is_text(b) and b.lstrip(b"\xef\xbb\xbf")[:13] == b"<?xml version" and b.rstrip()[-1] == 62 # 62 is `>`

def is_html(b:bytes):
    return is_text(b) and b.lstrip(b"\xef\xbb\xbf")[0] == b"<" and b"<html" in b or b"<!doctype" in b and  b.rstrip()[-1] == 62 # 62 is `>`

def is_json(b:bytes):
    return is_text(b) and b.lstrip(b"\xef\xbb\xbf")[0] == b"{" and b":" in b and b.rstrip()[-1] == 125 # 125 is `}`

sniffer=csv.Sniffer()
sniffer.preferred = [',', '\t', ';', ' ', ':', '|']
def is_csv(b:bytes):
    try:
        e = detect(b)["encoding"]
        d = sniffer.sniff(b.decode(e) if e else b.decode())
        return d.delimiter in sniffer.preferred
    except csv.Error:
        return False

def is_dml(b:bytes):
    try:
        return is_text(b) and b";" in b and (b.index(b"record") < b.index(b"end"))
    except ValueError:
        return False

""" referenced by
https://en.m.wikipedia.org/wiki/List_of_file_signatures
"""
match = { # bytes regex match define
  b'FO': [
          [re.compile(b'FORM....AIFF').match, 'aiff'],
	      [re.compile(b'FORM....ANBM').match, 'anbm'],
	      [re.compile(b'FORM....ANIM').match, 'anim'],
	      [re.compile(b'FORM....CMUS').match, 'cmus'],
	      [re.compile(b'FORM....FANT').match, 'fant'],
	      [re.compile(b'FORM....FAXX').match, 'faxx'],
	      [re.compile(b'FORM....FTXT').match, 'ftxt'],
	      [re.compile(b'FORM....ILBM').match, 'ilbm'],
	      [re.compile(b'FORM....SMUS').match, 'smus'],
	      [re.compile(b'FORM....YUVN').match, 'yuvn']],

  b'RI': [
          [re.compile(b'RIFF....AVI ').match, 'avi'],
	      [re.compile(b'RIFF....WAVE').match, 'wav'],
	      [re.compile(b'RIFF....WEBP').match, 'webp']],

  b'\xff\xd8': [
          [re.compile(b'\xff\xd8\xff\xdb\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\xff\xd8\xff\xee\xff\xd8\xff\xe1..Exif\x00\x00').match, 'jpg']],
 }

start = { # bytes startswith define
  b'\x00\x00': [
          [b'\x00\x00\x01\x00', 'icon'],
          [b'\x00\x00\x01\xba', 'mpg']],
  b'\x00\x01': [
          [b'\x00\x01\x00\x00Standard ACE DB\x00', 'accdb'],
          [b'\x00\x01\x00\x00Standard Jet DB\x00', 'mdb'],
          [b'\x00\x01\x00\x00', 'palmdata'],
          [b'\x00\x01BD', 'palmarchivedata'],
          [b'\x00\x01DT', 'palmcalenderdata']],
  b'\x00a': [[b'\x00asm', 'asm']],
  b'\x04"': [[b'\x04"M\x18', 'lz4']],
  b'\x05\x07': [[b'\x05\x07\x00\x00BOBO\x05\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01', 'cwk']],
  b'\x06\x07': [[b'\x06\x07\xe1\x00BOBO\x06\x07\xe1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01', 'cwk']],
  b'\n\r': [[b'\n\r\r\n', 'pcapng']],
  b'\x1aE': [[b'\x1aE\xdf\xa3', 'mkv']],
  b'\x1bL': [[b'\x1bLua', 'luac']],
  b'\x1f\x8b': [[b'\x1f\x8b', 'gz']],
  b'\x1f\x9d': [[b'\x1f\x9d', 'Z']],
  b'\x1f\xa0': [[b'\x1f\xa0', 'Z']],
  b' \x02': [[b' \x02\x01b\xa0\x1e\xab\x07\x02\x00\x00\x00', 'tde']],
  b'!<': [[b'!<arch>', 'linux deb file']],
  b'$S': [[b'$SDI0001', 'System Deployment Image']],
  b'%!': [[b'%!PS', 'ps']],
  b'%P': [[b'%PDF-', 'pdf']],
  b"'\x05": [[b"'\x05\x19V", 'U-Boot / uImage. Das U-Boot Universal Boot Loader.']],
  b'(\xb5': [[b'(\xb5/\xfd', 'Z']],
  b'0&': [[b'0&\xb2u\x8ef\xcf\x11\xa6\xd9\x00\xaa\x00b\xcel', 'asf']],
  b'0\x82': [[b'0\x82', 'der']],
  b'7H': [[b'7H\x03\x02\x00\x00\x00\x00X509KEY', 'kdb']],
  b'7z': [[b"7z\xbc\xaf'\x1c", '7z']],
  b'8B': [[b'8BPS', 'psd']],
  b':)': [[b':)\n', 'Smile file']],
  b'AG': [[b'AGD3', 'fh8']],
  b'BA': [[b'BACKMIKEDISK', 'File or tape containing a backup done with AmiBack on an Amiga. It typically is paired with an index file ']],
  b'BM': [[b'BM', 'bmp']],
  b'BP': [[b'BPG\xfb', 'Better Portable Graphics format']],
  b'BZ': [[b'BZh', 'bz2']],
  b'CW': [[b'CWSFWS', 'swf']],
  b'Cr': [[b'Cr24', 'Google Chrome extension']],
  b'DC': [[b'DCM\x01PA30', 'Windows Update Binary Delta Compression']],
  b'EM': [[b'EMU3', 'Emulator III synth samples'], [b'EMX2', 'Emulator Emaxsynth samples']],
  b'ER': [[b'ER\x02\x00\x00\x00\x8bER\x02\x00\x00\x00', 'Roxio Toast disc image file']],
  b'FL': [[b'FLIF', 'Free Lossless Image Format']],
  b'GI': [[b'GIF87aGIF89a', 'gif']],
  b'ID': [[b'ID3', 'mp3']],
  b'II': [[b'II*\x00', 'tiff'],
          [b'II*\x00\x10\x00\x00\x00CR', 'Canon RAW Format Version 2']],
  b'IN': [[b'INDX', 'Index file to a file or tape containing a backup done with AmiBack on an Amiga.']],
  b'L\x00': [[b'L\x00\x00\x00', 'lnk']],
  b'KD': [[b'KDM', 'vmdk']],
  b'LZ': [[b'LZIP', 'lzip']],
  b'MI': [[b'MIL ', '"SEAN\xa0: Session Analysis" Training file. Also used in compatible software "Rpw\xa0: Rowperfect for Windows" and "RP3W\xa0: ROWPERFECT3 for Windows".']],
  b'ML': [[b'MLVI', 'Magic Lantern Video file']],
  b'MS': [[b'MSCF', 'cab']],
  b'MT': [[b'MThd', 'midi']],
  b'MZ': [[b'MZ', 'exe']],
  b'NE': [[b'NES\x1a', 'Nintendo Entertainment System ROM file']],
  b'OR': [[b'ORC', 'Apache ORC ']],
  b'Ob': [[b'Obj\x01', 'Apache Avro binary file format']],
  b'Og': [[b'OggS', 'Ogg']],
  b'PA': [[b'PAR1', 'Apache Parquet columnar file format']],
  b'PK': [[b'PK\x03\x04', 'zip'], [b'PK\x05\x06', 'zip empty archive'],
          [b'PK\x07\x08', 'zip spanned archive']],
  b'PM': [[b'PMOCCMOC', 'Windows Files And Settings Transfer Repository']],
  b'RN': [[b'RNC\x01RNC\x02', 'Compressed file using Rob Northen Compression ']],
  b'Ra': [[b'Rar!\x1a\x07\x00', 'rar'],
          [b'Rar!\x1a\x07\x01\x00', 'rar']],
  b'Re': [[b'Received', 'Email Message var5']],
  b'SE': [[b'SEQ6', 'RCFile columnar file format']],
  b'SI': [[b'SIMPLE  =                    T', 'Flexible Image Transport System ']],
  b'SP': [[b'SP01', 'Amazon Kindle Update Package ']],
  b'SQ': [[b'SQLite format 3\x00', 'sqlite3']],
  b'SZ': [[b"SZDD\x88\xf0'3", 'Microsoft compressed file in Quantum format']],
  b'TA': [[b'TAPE', 'Microsoft Tape Format']],
  b'TD': [[b'TDEF', 'Telegram Desktop Encrypted File'],
          [b'TDF$', 'Telegram Desktop File']],
  b'UU': [[b'UU\xaa\xaa', 'PhotoCap Vector']],
  b'XP': [[b'XPDS', 'SMPTE DPX image']],
  b'[Z': [[b'[ZoneTransfer]', 'Microsoft Zone Identifier for URL Security Zones']],
  b'bo': [[b'book\x00\x00\x00\x00mark\x00\x00\x00\x00', 'macOS file Alias']],
  b'bv': [[b'bvx2', 'LZFSE - Lempel-Ziv style data compression algorithm using Finite State Entropy coding. OSS by Apple.']],
  b'de': [[b'dex\n035\x00', 'Dalvik Executable']],
  b'e\x87': [[b'e\x87xV', 'PhotoCap Object Templates']],
  b'fL': [[b'fLaC', 'Free Lossless Audio Codec']],
  b'\x00m': [[b'\x00mlocate', "locate"]],
  b'to': [[b'tox3', 'Open source portable voxel file']],
  b'v/': [[b'v/1\x01', 'OpenEXR image']],
  b'wO': [[b'wOF2', 'WOFF File Format 2.0'],
          [b'wOFF', 'WOFF File Format 1.0']],
  b'x\x01': [[b'x\x01s\rbb`', 'dmg'],
             [b'x\x0178\x9c78\xda', 'No Compression/low Default Compression Best Compression']],
  b'xV': [[b'xV4', 'PhotoCap Template']],
  b'xa': [[b'xar!', 'eXtensible ARchive format']],
  b'{\\': [[b'{\\rtf1', 'rtf']],
  b'\x7fE': [[b'\x7fELF', 'Executable and Linkable Format']],
  b'\x80*': [[b'\x80*_\xd7', 'Kodak Cineon image']],
  b'\x80\x02': [[b'\x80\x02', 'pickle']],
  b'\x80\x03': [[b'\x80\x03', 'pickle']],
  b'\x80\x04': [[b'\x80\x04', 'pickle']],
  b'\x80\x05': [[b'\x80\x05', 'pickle']],
  b'\x89P': [[b'\x89PNG\r\n\x1a\n', 'Image encoded in the Portable Network Graphics format']],
  b'\x89H': [[b'\x89HDF\r\n\x1a\n', 'hdf5']],
  b'\x96\xd5': [[b'\x96\xd5u!', 'sarbin']],
  b'\xa1\xb2': [[b'\xa1\xb2\xc3\xd4\xd4\xc3\xb2\xa1', 'Libpcap File Format']],
  b'\xbe\xba': [[b'\xbe\xba\xfe\xca', 'palmcalenderdata']],
  b'\xca\xfe': [[b'\xca\xfe\xba\xbe', 'javaclass']],
  b'\xce\xfa': [[b'\xce\xfa\xed\xfe', 'Mach-O binary ']],
  b'\xcf\x84': [[b'\xcf\x84\x01', 'jpg']],
  b'\xcf\xfa': [[b'\xcf\xfa\xed\xfe', 'Mach-O binary ']],
  b'\xd0\xcf': [[b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1', 'Microsoft Office 2003older']],
  b'\xd4\xc3': [[b'\xd4\xc3\xb2\xa1', 'pcap']],
  b'\xed\xab': [[b'\xed\xab\xee\xdb', 'rpm ']],
  b'\xef\xbb': [[b'\xef\xbb\xbf', 'UTF-8 encoded Unicode byte order mark']],
  b'\xfd7': [[b'\xfd7zXZ\x00\x00', 'xz']],
  b'\xfe\xed': [[b'\xfe\xed\xfe\xed', 'JKS JavakeyStore']],
  b'\xff\xfb': [[b'\xff\xfb', 'mp3']],
  b'\xff\xfe': [[b'\xff\xfe', 'Byte-order mark for text file encoded in little-endian 16-bit Unicode Transfer Format'],
                [b'\xff\xfe\x00\x00', 'Byte-order mark for text file encoded in little-endian 32-bit Unicode Transfer Format']]
  }

def lookuptype(x:bytes):
    if is_bin(x):
        k = x[:2]
        if k == b"PK":
            if is_doc(x): return "docx"
            if is_xls(x): return "xlsx"
            if is_ppt(x): return "pptx"
        elif k == b"\xd0\xcf":
            if is_doc(x): return "doc"
            if is_xls(x): return "xls"
            if is_ppt(x): return "ppt"
        elif is_tar(x):
            return "tar"
        elif is_lha(x):
            return 'lha'
        try:
            if k in start:
                return next(d for s, d in start[k] if x.startswith(s))
            if k in match:
                return next(d for f, d in match[k] if f(x))
            return None
        except StopIteration:
            return None

    if is_xml(x): return "xml"
    if is_html(x): return "html"
    if is_json(x): return "json"
    if is_csv(x): return "csv"
    if is_dml(x): return "dml"

    return "txt"

def headtail(fp, buf):
    ret = fp.read(buf)
    try:
        fp.seek(-1 * buf, 2)
        ret += fp.read()
    except OSError:
        ret += fp.read()[-1 * buf:]
    return ret

def guesstype(f):
    buf = 65536
    check = lambda *tp: isinstance(f, tp)

    if hasattr(f, "seek"):
        pos = f.tell()

    ret = b""
    klass = f.__class__.__name__

    if check(str) or hasattr(f, "joinpath"):
        with open(f, "rb") as fp:
            ret = headtail(fp, buf)

    elif check(BytesIO) or klass in ["ExFileObject", "ZipExtFile"]:
        ret = headtail(f, buf)

    elif check(bytearray, bytes):
        ret = f[:buf] + f[-1 * buf:]

    elif check(StringIO):
        e = f.encoding
        f.seek(0)
        ret = headtail(f, buf)
        if e:
            ret = ret.encode(e)
        else:
            ret =  ret.encode()
    else:
        try:
            m = f.mode
        except AttributeError:
            m = f._mode
        if isinstance(m, int) or "b" in m:
            ret = headtail(f, buf)
        else:
            with open(f.name, mode=m + "b") as fp:
                ret = headtail(fp, buf)

    if hasattr(f, "seek"):
        f.seek(pos)

    if not ret:
        return "ZERO"

    _type = lookuptype(ret)
    if _type == "Microsoft Office 2003older":
        return os.path.splitext(hasattr(f, "name") and f.name or f)[1][1:]
    return _type

def test():
    from util.core import tdir
    from glob import glob
    from datetime import datetime as dt

    for g in glob(tdir+"*"):
        t=dt.now()
        basename = os.path.basename(g)
        print(basename, guesstype(g), dt.now()-t)

def main():
    import sys
    from glob import glob
    from argparse import ArgumentParser

    ps = ArgumentParser(prog="guesstype",
                        description="guess filetype program\n")
    padd = ps.add_argument

    padd('-V','--version', action='version', version='%(prog)s ' + __version__)
    padd("-v", "--verbose", help="print progress",
         action='store_true', default=False)
    padd("files",
         metavar="<files>",
         nargs="+",  default=[],
         help="text dump any files")

    args = ps.parse_args()

    def walk(args):
        for arg in args.files:
            for f in glob(arg):
                f = os.path.normpath(f)
                if args.verbose:
                    sys.stderr.write("Dumping:{}\n".format(f))
                    sys.stderr.flush()
                yield f

    i = None
    for i, f in enumerate(walk(args)):
        print(guesstype(f))

    if i is None:
        raise FileNotFoundError(str(args.files))

if __name__ == "__main__":
    # test()
    main()

