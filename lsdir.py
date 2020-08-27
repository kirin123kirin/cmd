#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

list directory infomation tools

MIT License

"""

__version__ = "0.0.3"
__author__ = "m.yama"


__all__ = ["fwalk", "dwalk", "filestree", "dirstree", "filemode"]


import sys
import os
from os.path import basename, splitext, normpath, isdir, dirname
from os import scandir, fspath

from datetime import datetime
from functools import lru_cache
from glob import glob

is_posix = os.name == "posix"
is_nt = os.name == "nt"

if is_posix:
    from pwd import getpwuid
    from grp import getgrgid

    @lru_cache()
    def getuser(uid):
        try:
            return getpwuid(uid).pw_name
        except KeyError:
            return uid

    @lru_cache()
    def getgroup(gid):
        try:
            return getgrgid(gid).gr_name
        except KeyError:
            return gid
else:
    def getuser(uid):
        return ""
    def getgroup(gid):
        return ""

from pathlib import Path, PureWindowsPath
import struct


# Link Format from MS: https://msdn.microsoft.com/en-us/library/dd871305.aspx
# Need to be able to read shortcut target from .lnk file on linux or windows.
# Original inspiration from: http://.com/questions/397125/reading-the-target-of-a-lnk-file-in-python
requiredCLSID   = b'\x01\x14\x02\x00\x00\x00\x00\x00\xC0\x00\x00\x00\x00\x00\x00\x46'  # puzzling

class MSShortcut():
    """
    interface to Microsoft Shortcut Objects.  Purpose:
    - I need to be able to get the target from a samba shared on a linux machine
    - Also need to get access from a Windows machine.
    - Need to support several forms of the shortcut, as they seem be created differently depending on the
      creating machine.
    - Included some 'flag' types in external interface to help test differences in shortcut types

    Args:
        scPath (str): path to shortcut

    Limitations:
        - There are some omitted object properties in the implementation.
          Only implemented / tested enough to recover the shortcut target information. Recognized omissions:
          - LinkTargetIDList
          - VolumeId structure (if captured later, should be a separate class object to hold info)
          - Only captured environment block from extra data
        - I don't know how or when some of the shortcut information is used, only captured what I recognized,
          so there may be bugs related to use of the information
        - NO shortcut update support (though might be nice)
        - Implementation requires python 3.4 or greater
        - Tested only with Unicode data on a 64bit little endian machine, did not consider potential endian issues

    Not Debugged:
        - localBasePath - didn't check if parsed correctly or not.
        - commonPathSuffix
        - CommonNetworkRelativeLink

    """

    def __init__(self, scPath):
        """
        Parse and keep shortcut properties on creation
        """
        self.scPath = Path(scPath)

        self.clsid = None
        self.lnkFlags = None
        self.lnkInfoFlags = None
        self.localBasePath = None
        self.commonPathSuffix = None
        self.CommonNetworkRelativeLink = None
        self.name = None
        self.relativePath = None
        self.workingDir = None
        self.commandLineArgs = None
        self.iconLocation = None
        self.envTarget = None

        self._ParseLnkFile(self.scPath)


    @property
    def targetPath(self):
        """
        Args:
            woAnchor (bool): remove the anchor (\serverpath or drive:) from returned path.

        Returns:
            a libpath PureWindowsPath object for combined workingDir/relative path
            or the envTarget

        Raises:
            ValueError when no target path found in Shortcut
        """
        target = None
        if self.workingDir:
            target = PureWindowsPath(self.workingDir)
            if self.relativePath:
                target = target / PureWindowsPath(self.relativePath)
            else: target = None

        if not target and self.envTarget:
            target = PureWindowsPath(self.envTarget)

        if not target:
            raise ValueError("Unable to retrieve target path from MS Shortcut: shortcut = {}"
                               .format(str(self.scPath)))

        return target

    @property
    def targetPathWOAnchor(self):
        tp = self.targetPath
        return tp.relative_to(tp.anchor)


    def _ParseLnkFile(self, lnkPath):
        with lnkPath.open('rb') as f:
            content = f.read()

            # verify size  (4 bytes)
            hdrSize = struct.unpack('I', content[0x00:0x04])[0]
            if hdrSize != 0x4C:
                raise ValueError("MS Shortcut HeaderSize = {}, but required to be = {}: shortcut = {}"
                                   .format(hdrSize, 0x4C, str(lnkPath)))

            # verify LinkCLSID id (16 bytes)
            self.clsid = bytes(struct.unpack('B'*16, content[0x04:0x14]))
            if self.clsid != requiredCLSID:
                raise ValueError("MS Shortcut ClassID = {}, but required to be = {}: shortcut = {}"
                                   .format(self.clsid, requiredCLSID, str(lnkPath)))

            # read the LinkFlags structure (4 bytes)
            self.lnkFlags = struct.unpack('I', content[0x14:0x18])[0]

            position = 0x4C

            # if HasLinkTargetIDList bit, then position to skip the stored IDList structure and header
            if (self.lnkFlags & 0x01):
                idListSize = struct.unpack('H', content[position:position+0x02])[0]
                position += idListSize + 2

            # if HasLinkInfo, then process the linkinfo structure
            if (self.lnkFlags & 0x02):
                (linkInfoSize, linkInfoHdrSize, self.linkInfoFlags,
                 volIdOffset, localBasePathOffset,
                 cmnNetRelativeLinkOffset, cmnPathSuffixOffset) = struct.unpack('IIIIIII', content[position:position+28])

                # check for optional offsets
                localBasePathOffsetUnicode = None
                cmnPathSuffixOffsetUnicode = None
                if linkInfoHdrSize >= 0x24:
                    (localBasePathOffsetUnicode, cmnPathSuffixOffsetUnicode) = struct.unpack('II', content[position+28:position+36])

                # if info has a localBasePath
                if (self.linkInfoFlags & 0x01):
                    bpPosition = position + localBasePathOffset

                    # not debugged - don't know if this works or not
                    self.localBasePath = _UnpackZ('z', content[bpPosition:])[0].decode('ascii')


                    if localBasePathOffsetUnicode:
                        bpPosition = position + localBasePathOffsetUnicode
                        self.localBasePath = _UnpackUnicodeZ('z', content[bpPosition:])[0]
                        self.localBasePath = self.localBasePath.decode('utf-16')


                # get common Path Suffix
                cmnSuffixPosition = position + cmnPathSuffixOffset
                self.commonPathSuffix = _UnpackZ('z', content[cmnSuffixPosition:])[0].decode('ascii')

                if cmnPathSuffixOffsetUnicode:
                    cmnSuffixPosition = position + cmnPathSuffixOffsetUnicode
                    self.commonPathSuffix = _UnpackUnicodeZ('z', content[cmnSuffixPosition:])[0]
                    self.commonPathSuffix = self.commonPathSuffix.decode('utf-16')



                # check for CommonNetworkRelativeLink
                if (self.linkInfoFlags & 0x02):
                    relPosition = position + cmnNetRelativeLinkOffset
                    self.CommonNetworkRelativeLink = _CommonNetworkRelativeLink(content, relPosition)

                position += linkInfoSize

            # If HasName
            if (self.lnkFlags & 0x04):
                (position, self.name) = self.readStringObj(content, position)


            # get relative path string
            if (self.lnkFlags & 0x08):
                (position, self.relativePath) = self.readStringObj(content, position)


            # get working dir string
            if (self.lnkFlags & 0x10):
                (position, self.workingDir) = self.readStringObj(content, position)


            # get command line arguments
            if (self.lnkFlags & 0x20):
                (position, self.commandLineArgs) = self.readStringObj(content, position)


            # get icon location
            if (self.lnkFlags & 0x40):
                (position, self.iconLocation) = self.readStringObj(content, position)


            # look for environment properties
            if (self.lnkFlags & 0x200):
                while True:
                    size = struct.unpack('I', content[position:position+4])[0]

                    if size==0: break

                    signature = struct.unpack('I', content[position+4:position+8])[0]


                    # EnvironmentVariableDataBlock
                    if signature == 0xA0000001:

                        if (self.lnkFlags & 0x80): # unicode
                            self.envTarget = _UnpackUnicodeZ('z', content[position+268:])[0]
                            self.envTarget = self.envTarget.decode('utf-16')
                        else:
                            self.envTarget = _UnpackZ('z', content[position+8:])[0].decode('ascii')



                    position += size


    def readStringObj(self, scContent, position):
        strg = ''
        size = struct.unpack('H', scContent[position:position+2])[0]

        if (self.lnkFlags & 0x80): # unicode
            size *= 2
            strg = struct.unpack(str(size)+'s', scContent[position+2:position+2+size])[0]
            strg = strg.decode('utf-16')
        else:
            strg = struct.unpack(str(size)+'s', scContent[position+2:position+2+size])[0].decode('ascii')

        position += size + 2 # 2 bytes to account for CountCharacters field

        return (position, strg)




class _CommonNetworkRelativeLink():

    def __init__(self, scContent, linkContentPos):

        self.networkProviderType = None
        self.deviceName = None
        self.netName = None

        (linkSize, flags, netNameOffset,
         devNameOffset, self.networkProviderType) = struct.unpack('IIIII', scContent[linkContentPos:linkContentPos+20])


        if netNameOffset > 0x014:
            (netNameOffsetUnicode, devNameOffsetUnicode) = struct.unpack('II', scContent[linkContentPos+20:linkContentPos+28])

            self.netName = _UnpackUnicodeZ('z', scContent[linkContentPos+netNameOffsetUnicode:])[0]
            self.netName = self.netName.decode('utf-16')
            self.deviceName = _UnpackUnicodeZ('z', scContent[linkContentPos+devNameOffsetUnicode:])[0]
            self.deviceName = self.deviceName.decode('utf-16')
        else:
            self.netName = _UnpackZ('z', scContent[linkContentPos+netNameOffset:])[0].decode('ascii')
            self.deviceName = _UnpackZ('z', scContent[linkContentPos+devNameOffset:])[0].decode('ascii')



def _UnpackZ (fmt, buf) :
    while True :
        pos = fmt.find ('z')
        if pos < 0 :
            break
        z_start = struct.calcsize (fmt[:pos])
        z_len = buf[z_start:].find(b'\0')

        fmt = '%s%dsx%s' % (fmt[:pos], z_len, fmt[pos+1:])

    fmtlen = struct.calcsize(fmt)
    return struct.unpack (fmt, buf[0:fmtlen])


def _UnpackUnicodeZ (fmt, buf) :
    while True :
        pos = fmt.find ('z')
        if pos < 0 :
            break
        z_start = struct.calcsize (fmt[:pos])
        # look for null bytes by pairs
        z_len = 0
        for i in range(z_start,len(buf),2):
            if buf[i:i+2] == b'\0\0':
                z_len = i-z_start
                break

        fmt = '%s%dsxx%s' % (fmt[:pos], z_len, fmt[pos+1:])

    fmtlen = struct.calcsize(fmt)
    return struct.unpack (fmt, buf[0:fmtlen])

@lru_cache()
def ts2date(x, dfm = "%Y/%m/%d %H:%M"):
    return datetime.fromtimestamp(x).strftime(dfm)


def fwalk(top, exclude=None, followlinks=False):
    scandir_it = scandir(fspath(top))

    with scandir_it:
        while True:
            try:
                entry = next(scandir_it)
                if exclude and entry.name in exclude:
                    continue
            except StopIteration:
                break

            try:
                is_dir = entry.is_dir()
            except OSError:
                is_dir = False

            if is_dir:
                if followlinks and entry.is_symlink():
                    yield from fwalk(os.readlink(entry.path), exclude, followlinks)
                else:
                    yield from fwalk(entry.path, exclude, followlinks)
            else:
                yield entry


def dwalk(top, exclude=None, followlinks=False):
    scandir_it = scandir(fspath(top))

    with scandir_it:
        while True:
            try:
                entry = next(scandir_it)
                if exclude and entry.name in exclude:
                    continue
            except StopIteration:
                break

            try:
                is_dir = entry.is_dir()
            except OSError:
                is_dir = False

            if is_dir:
                if followlinks and entry.is_symlink():
                    yield from dwalk(os.readlink(entry.path), exclude, followlinks)
                else:
                    yield from dwalk(entry.path, exclude, followlinks)

_filemode_table = (
    ((0o120000,      "l"),
     (0o140000,      "s"),
     (0o100000,      "-"),
     (0o060000,      "b"),
     (0o040000,      "d"),
     (0o020000,      "c"),
     (0o010000,      "p")),

    ((0o0400,        "r"),),
    ((0o0200,        "w"),),
    ((0o0100|0o4000, "s"),
     (0o4000,        "S"),
     (0o0200,        "x")),

    ((0o0040,        "r"),),
    ((0o0200,        "w"),),
    ((0o0010|0o2000, "s"),
     (0o2000,        "S"),
     (0o0010,        "x")),

    ((0o0004,       "r"),),
    ((0o0002,       "w"),),
    ((0o0001|0o1000,"t"),
     (0o1000,       "T"),
     (0o0001,       "x"))
)

@lru_cache()
def filemode(mode):
    perm = []
    add = perm.append
    for table in _filemode_table:
        for bit, char in table:
            if mode & bit == bit:
                add(char)
                break
        else:
            add("-")
    return "".join(perm)


def fileattr(f, stat=None, followlinks=False, result_type=None):
    stat = stat or os.stat(f)
    mode = stat.st_mode
    link = ""
    if followlinks is False:
        try:
            if f.endswith(".lnk"):
                link = str(MSShortcut(f).targetPath)
            elif mode & 0o120000 == 0o120000:
                link = os.readlink(f)
        except (ValueError, OSError) as e:
                link = "[{}]: {}".format(type(e).__name__, e)

    return (
            filemode(mode),
            getuser(stat.st_uid),
            getgroup(stat.st_gid),
            ts2date(stat.st_mtime),
            result_type(stat.st_size) if result_type else stat.st_size,
            splitext(f)[1],
            basename(f),
            f,
            link,
            *dirname(f).replace("\\", "/").strip("/").split("/"),
            )

def _tree(func, fn, exclude=None, followlinks=False, header=True):

    i = 0

    if os.path.isdir(fn):
        fn += "/*"

    for g in glob(normpath(fn)):
        if i == 0 and header:
            yield ["mode", "uname", "gname", "mtime", "size", "ext", "name", "fullpath", "link", "dirnest"]

        if isdir(g):
            for f in func(g, exclude, followlinks):
                if func is fwalk and f.name.startswith("~$"):
                    continue

                yield fileattr(f.path, f.stat(), followlinks=followlinks, result_type=str)
                i += 1

        elif func is fwalk and basename(g).startswith("~$"):
            continue

        else:
            yield fileattr(g, followlinks=followlinks, result_type=str)
            i += 1

    if i == 0:
        raise FileNotFoundError(fn)

def filestree(fn, exclude=None, followlinks=False, header=True):
    return _tree(fwalk, fn=fn, exclude=exclude, followlinks=followlinks, header=header)

def dirstree(fn, exclude=None, followlinks=False, header=True):
    return _tree(dwalk, fn=fn, exclude=exclude, followlinks=followlinks, header=header)

def unicode_escape(x):
    return x.encode().decode("unicode_escape")

def create_parser():
    from argparse import ArgumentParser
    import io
    import codecs

    parser = ArgumentParser(description="main templace")
    padd = parser.add_argument

    padd('-o', '--outfile',
         type=lambda f: lambda e: codecs.open(f, mode="w", encoding=e, errors="backslashreplace"),
         help='outputfile path',
         default=lambda e: io.TextIOWrapper(sys.stdout.buffer, encoding=e, errors="backslashreplace"),
    )

    padd('-t',
        '--type',
        help='filter file type , file=>f or dir=>d (default file)',
        default="f")

    padd('-s', '--sep',
         type=unicode_escape,
         help='output separator',
         default="\t",
    )

    padd('-E', '--exclude', nargs='+',
         help='exclude files',
         default=[".svn", ".git", "old", "bak"],
    )

    padd('-N', '--noheader',
         action='store_false', default=True,
         help='output no header',
    )

    padd('-e', '--encoding',
        help='output encoding',
        default=os.name == "nt" and "cp932" or "utf-8",
    )

    padd('filename',
         metavar='<filename>',
         nargs="+",
         help='Target Files',
    )

    return parser.parse_args()

def main_file(args):
    with args.outfile(args.encoding) as outfile:
        for i, filename in enumerate(args.filename):
            for row in filestree(filename, exclude=args.exclude, header=args.noheader and i == 0):
                print(args.sep.join(row), file=outfile)

def main_dir(args):
    with args.outfile(args.encoding) as outfile:
        for i, filename in enumerate(args.filename):
            for row in dirstree(filename, exclude=args.exclude, header=args.noheader and i == 0):
                print(args.sep.join(row), file=outfile)

def main():
    args = create_parser()
    tp = args.type.lower()[0]

    if tp == "f":
        main_file(args)
    elif tp == "d":
        main_dir(args)
    else:
        raise ValueError("Unknown --type `{}`\n  Please `--type file` or `--type dir`".format(tp))


def test():

    def test_filestree():
        i = 0
        for i, x in enumerate(filestree(".")):
            print(x)
            pass

        assert i > 0

#    def test_benchmark():
#        list(filestree(r"C:/temp"))

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = datetime.now()
            func()
            t2 = datetime.now()
            print("{} : time {}".format(x, t2-t1))


if __name__ == '__main__':
    # test()
    main()



