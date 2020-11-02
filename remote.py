#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from os.path import join as pathjoin, normpath, dirname, basename, exists, isdir
from os import makedirs, getcwd
import paramiko
from scp import SCPClient

__all__ = [
    "sshclient",
    "scpget",
    "sshcmd",
    "httprequest",
    "svnls",
    "svnget",
]

def _prog_dl(target, dlsize, completesize):
    if completesize == 0:
        print("Downloading:", normpath(target))
    elif completesize == dlsize:
        print("\t{:,.2f} KByte Done.".format(dlsize / 1024))
    else:
        print('\t{:,.2f} KByte) ({:>6.2f}%)'.format(
            dlsize / 1024, completesize / dlsize * 100))

def _prog_up(target, dlsize, completesize):
    if completesize == 0:
        print("Uploading:", normpath(target))
    elif completesize == dlsize:
        print("\t{:,.2f} KByte Done.".format(dlsize / 1024))
    else:
        print('\t{:,.2f} KByte) ({:>6.2f}%)'.format(
            dlsize / 1024, completesize / dlsize * 100))

def listify(x):
    if not x:
        return []
    elif isinstance(x, list):
        return x
    elif isinstance(x, (str, bytes, bytearray, int, float, bool, )):
        return [x]
    elif hasattr(x, "__iter__") or hasattr(x, "__next__"):
        return list(x)
    else:
        return [x]


def sshclient(*args, **kw):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(*args, **kw)
    return ssh


def scpget(server, user, password, remote_path, target_dir=getcwd(),
           recursive=True, mkdirs=True, port=22, key_filename=None,
           hieralchy=False, verbose=True):
    """
    scpget oneliner

    Parameters
    ----------
    server : str
        connecting server ipaddress or hostname
    user : str
        sshuser
    password : str
        sshpassword
    remote_path : str or list of str
        get copy target remote file path.
    target_dir : str
        download tareget directory.
    recursive : boolean, optional
        get recursive directory. The default is True.
    mkdirs : boolean, optional
        if not exists directory force make directories. The default is True.
    port : int, optional
        ssh port. The default is 22.
    key_filename : str, optional
        ssh key filefilepath. The default is None.
    hieralchy : TYPE, optional
        local download make hieralchy directory. The default is False.
    verbose : TYPE, optional
        print progress. The default is True.

    Raises
    ------
    FileExistsError
        DESCRIPTION.

    Returns
    -------
    None.

    """

    if exists(target_dir) and not isdir(target_dir):
        raise FileExistsError("same file name already exists")

    with sshclient(server, port, user, password, key_filename=key_filename) as ssh:
        with SCPClient(ssh.get_transport(),
                       progress=_prog_dl if verbose else None,
                       sanitize=lambda x: x) as scp:

            for rp in listify(remote_path):
                td = target_dir
                if hieralchy:
                    td = pathjoin(
                        target_dir,
                        server,
                        dirname(rp).lstrip("/"))
                if mkdirs:
                    makedirs(td, exist_ok=True)
                scp.get(rp, td, recursive, preserve_times=True)

def scpput(server, user, password, local_path, remote_path,
           recursive=True, port=22, key_filename=None,
           verbose=True):
    """
    scpput oneliner

    Parameters
    ----------
    server : str
        connecting server ipaddress or hostname
    user : str
        sshuser
    password : str
        sshpassword
    local_path : str or list of str
        put copy target local file path.
    remote_path : str
        put tareget remote directory.
    recursive : boolean, optional
        put recursive directory. The default is True.
    port : int, optional
        ssh port. The default is 22.
    key_filename : str, optional
        ssh key filefilepath. The default is None.
    verbose : TYPE, optional
        print progress. The default is True.

    Raises
    ------
    FileExistsError
        DESCRIPTION.

    Returns
    -------
    None.

    """

    if exists(remote_path) and not isdir(remote_path):
        raise FileExistsError("same file name already exists")

    with sshclient(server, port, user, password, key_filename=key_filename) as ssh:
        with SCPClient(ssh.get_transport(),
                       progress=_prog_up if verbose else None,
                       sanitize=lambda x: x) as scp:

            scp.put(local_path, remote_path, recursive, preserve_times=True)


def sshcmd(command, server, user, password=None, port=22, key_filename=None, bufsize=-1):
    """
    Return:
        stdout
    Error:
        RuntimeError print stderr
    """
    command = command.replace("\n", "; ")
    with sshclient(server, port, user, password, key_filename=key_filename) as ssh:
        stdin, stdout, stderr = ssh.exec_command(command, bufsize=bufsize)

        for line in stdout:
            yield line

        err = "".join(e for e in stderr if not (e.startswith("stty:") and "ioctl" in e))
        if err:
            raise RuntimeError(err)


class NotInstalledModuleError(Exception):
    def stderr(self):
        raise __class__("** {} **".format(*self.args)) if self.args else __class__
    def __call__(self, *args, **kw): self.stderr()
    def __getattr__(self, *args, **kw): self.stderr()

try:
    import requests
    from requests.auth import HTTPBasicAuth, HTTPDigestAuth
    import csv
    
    from fnmatch import fnmatch
    
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    try:
        from lxml.html import fromstring
    except ModuleNotFoundError:
        from xml.etree.ElementTree import fromstring
    
    class httprequest(object):
        def __init__(self, url, username=None, password=None, proxies={}, timeout=5):
            self.url = url
            self.username = username
            self.password = password
    
            self.kw = dict(timeout=timeout)
    
            self.session = requests.Session()
            self.session.trust_env = False
            self.session.proxies=proxies
            self.session.verify=False
            ua = "Mozilla/5.0 (Windows NT x.y; Win64; x64; rv:10.0) Gecko/20100101 Firefox/10.0"
            self.session.headers={"User-Agent":ua}
    
            self.retcode = None
            self._res = None
            self.get()
    
        def get(self, url=None, ignore_error=False):
            url = url or self.url
            self._res = self.session.get(url, **self.kw)
    
            if self._res.status_code == 401 and self.username:
                self.kw["auth"] = HTTPBasicAuth(self.username, self.password)
                self._res = self.session.get(url, **self.kw)
                if self._res.status_code == 401:
                    self.kw["auth"] = HTTPDigestAuth(self.username, self.password)
                    self._res = self.session.get(url, **self.kw)
    
            self.retcode = self._res.status_code
            if not ignore_error:
                self._res.raise_for_status()
    
            return self._res
    
        @property
        def res(self):
            if self._res is None:
                self.get()
            return self._res
    
        @property
        def headers(self):
            return self.res.headers
    
        @property
        def text(self):
            return self.res.text
    
        def parse(self):
            ct = self.headers["Content-Type"].split(";")[0]
            if ct in ["text/html", "text/xml"]:
                return fromstring(self.res.text)
            elif ct == "text/csv":
                return csv.reader(self.res.text.splitlines())
            elif ct == "text/plain":
                return self.res.text
            else:
                return self.res.content
    
    
        def download(self, output, url=None):
            if url:
                self.get(url)
            with output if hasattr(output, "write") else open(output, "wb") as w:
                w.write(self.res.content)
    
        def close(self):
            self.session.close()
            self._res = None
    
    
        def __enter__(self):
            return self
    
        def __exit__(self, ex_type, ex_value, trace):
            self.close()
            if ex_type:
                print(ex_type, ex_value, trace, file=sys.stderr)

except ModuleNotFoundError:
    httprequest = NotInstalledModuleError("Please Install command: pip install requests")



try:
    from svn.remote import RemoteClient
    svncommad = 'svn'
    
    def svnclient(url, username=None, password=None):
        return RemoteClient(url, username=username, password=password,
            svn_filepath=svncommad, trust_cert=True)
    
    def svnls(url, username=None, password=None):
        client = svnclient(url, username=username, password=password)
        return client.list()
    
    def svnget(url, username=None, password=None, outdir="."):
        urldir = dirname(url)
        for x in svnls(urldir, username=username, password=password):
            if fnmatch(x, basename(url)):
                target = "/".join([urldir, x])
                outfile = pathjoin(outdir, x)
                svnclient(target).export(outfile)
                yield outfile

except ModuleNotFoundError:
    svnls = svnget = NotInstalledModuleError("Please Install command: pip install svn")


