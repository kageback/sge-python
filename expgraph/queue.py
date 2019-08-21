import subprocess
import os
import socket
import expgraph.rsync as rsync
from expgraph.misc import *
import expgraph.function_caller as function_caller

# Deprecated!! This file is only keept to enable the use of old save files. New development is to be done in ge_queue.py

class Queue:
    def __init__(self, cluster_wd='~/runtime/env/', interpreter='python3', interpreter_args='-u',
                 ge_gpu=-1, ge_aux_args='', host='localhost', user='', queue_limit=1):

        self.local_wd = './'
        self.exclude = ['__pycache__', '.git', '.idea']

        if not host == 'localhost':
            # check if host is actually localhost. Host needs to be in /etc/hosts for this to work
            if socket.gethostbyname('localhost') == socket.gethostbyname(host):
                host = 'localhost'

        self.cluster_wd = os.path.expanduser(cluster_wd) if host == 'localhost' else cluster_wd
        self.cluster_wd = enforce_trailing_backslash(self.cluster_wd)

        self.interpreter_cmd = [interpreter, interpreter_args]
        self.host = host
        if user != '':
            self.host = user + '@' + self.host
        self.queue_limit = queue_limit

        self.qsub_base_args = 'qsub -b y -wd ' + self.cluster_wd + ' ' + ge_aux_args
        if ge_gpu >= 0:
            self.qsub_base_args += ' -l gpu=' + str(ge_gpu)

        # create cluster wd if necessary
        cmd = ['mkdir', '-p', self.cluster_wd]
        proc = subprocess.Popen(select_shell(cmd, self.host))
        proc.wait()

    def sync(self, local_path, cluster_path, sync_to, exclude=[], recursive=False):
        exclude += self.exclude
        rsync.sync_folder(local_path, self.cluster_wd + cluster_path, sync_to, exclude=exclude, remote_host=self.host, recursive=recursive)

    def submit_job(self, task_name, args, log_folder="./", dependencies=[]):
        log_folder = enforce_trailing_backslash(log_folder)
        task_args = ['-N', task_name,
                     '-o', log_folder + task_name + '.log',
                     '-e', log_folder + task_name + '.error']

        if dependencies != []:
            task_args += ['-hold_jid', ','.join([str(dep) for dep in dependencies])]

        qsub_cmd = self.qsub_base_args.split() + task_args + self.interpreter_cmd + args

        proc = subprocess.Popen(select_shell(qsub_cmd, self.host), stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()

        out = stdout.split()
        sge_job_id = int(out[2])
        return sge_job_id

    def queue_stat(self, sge_job_ids=[]):
        states = {}
        proc = subprocess.Popen(select_shell(['qstat'], self.host), stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        lines = stdout.split(b'\n')

        for line in lines[2:-1]:
            sge_job_id = int(line.split()[0])
            if sge_job_ids == [] or (sge_job_id in sge_job_ids):
                task_state = line.split()[4].decode()
                if task_state in states.keys():
                    states[task_state] += 1
                else:
                    states[task_state] = 1
        return states

    def is_job_finished(self, sge_job_id):
        states = self.queue_stat(sge_job_ids=[sge_job_id])
        return len(states) == 0

    def queue_slots_available(self):
        # Untested
        other = 0
        states = self.queue_stat()
        for state, n in states.items():
            if state == b'qw':
                queued = n
            elif state == b'r':
                running = n
            else:
                other += n

        return self.queue_limit - queued


class Local(Queue):
    def sync(self, local_path, cluster_path, sync_to, exclude=[], recursive=True):
        # print('Sync not implemented for local queue.')
        pass

    def queue_slots_available(self):
        return 1

    def queue_stat(self, sge_job_ids=None):
        return {}

    def submit_job(self, task_name, args, log_folder="./", dependencies=[]):
        function_caller.main(args[1:])
        return 1

    def is_job_finished(self, sge_job_id):
        return True
