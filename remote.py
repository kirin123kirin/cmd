#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os.path import join as pathjoin, normpath, dirname, exists, isdir
from os import makedirs, getcwd
import paramiko
from scp import SCPClient

__all__ = [
    "sshclient",
    "scpget",
    "sshcmd",
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


def main_scpget():
    import fire
    fire.Fire(scpget)

if __name__ == "__main__":
    main_scpget()
