import subprocess


def sync_folder(local_path, remote_path, remote_host='localhost', local_to_remote=True, skip_folders=[], aux_args=""):
    cmd = ['rsync', '-vr'] + _shell(remote_host) + aux_args.split()

    for folder in skip_folders:
        cmd += ['--exclude', folder]

    if local_to_remote:
        cmd += [local_path, _host_path(remote_path, remote_host)]
    else:
        cmd += [_host_path(remote_path, remote_host), local_path]

    process = subprocess.Popen(cmd)
    process.wait()


def _host_path(path, host):
    if host == 'localhost':
        return path
    else:
        return host + ':' + path


def _shell(host):
    if host == 'localhost':
        return []
    else:
        return ['-e ssh']