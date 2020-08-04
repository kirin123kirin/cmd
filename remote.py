#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os.path import join as pathjoin, normpath, dirname, exists, isdir
from os import makedirs
import paramiko
from scp import SCPClient

def _prog(target, dlsize, completesize):
    if completesize == 0:
        print("Downloading:", normpath(target))
    elif completesize == dlsize:
        print("\t{:,.2f} KByte Done.".format(dlsize / 1024))
    else:
        print('\t{:,.2f} KByte) ({:>6.2f}%)'.format(
            dlsize / 1024, completesize / dlsize * 100))

def sshclient(*args, **kw):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(*args, **kw)
    return ssh


def scpget(server, user, password, remote_path, target_dir,
           recursive=True, port=22, key_filename=None,
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
    remote_path : str
        get copy target remote file path.
    target_dir : str
        download tareget directory.
    recursive : boolean, optional
        get recursive directory. The default is True.
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

    if hieralchy:
        target_dir = pathjoin(target_dir,
                              server,
                              dirname(remote_path).lstrip("/"))

    with sshclient(server, port, user, password, key_filename=key_filename) as ssh:
        with SCPClient(ssh.get_transport(),
                       progress=_prog if verbose else None,
                       sanitize=lambda x: x) as scp:

            makedirs(target_dir, exist_ok=True)
            scp.get(remote_path, target_dir, recursive)

def sshcmd(server, user, password, command, port=22, key_filename=None, verbose=True):
    """
    Return:
        stdin, stdout, stderr
    """
    with sshclient(server, port, user, password, key_filename=key_filename) as ssh:
        return ssh.exec_command(command)


def main_scpget():
    import fire
    fire.Fire(scpget)

if __name__ == "__main__":
    main_scpget()