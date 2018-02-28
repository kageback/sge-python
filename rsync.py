import subprocess
import os


from enum import Enum
class SyncTo(Enum):
    REMOTE = 1
    LOCAL = 2

def sync_folder(local_path, remote_path, sync_to, remote_host='localhost', exclude=[], aux_args=""):
    cmd = ['rsync', '-vr'] + _shell(remote_host) + aux_args.split()

    for folder in exclude:
        cmd += ['--exclude', folder]

    if sync_to is SyncTo.REMOTE:
        cmd += [local_path, _host_path(remote_path, remote_host)]
    elif sync_to is SyncTo.LOCAL:
        cmd += [_host_path(remote_path, remote_host), local_path]

    process = subprocess.Popen(cmd)
    process.wait()


def _host_path(path, host, user=''):
    if host == 'localhost':
        return os.path.expanduser(path)
    else:
        return host + ':' + path


def _shell(host):
    if host == 'localhost':
        return []
    else:
        return ['-e ssh']
