import subprocess
import os
import socket
import gridengine.rsync as rsync
from gridengine.misc import *
import gridengine.function_caller as function_caller

#import urllib
#local_external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')

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

        self.interpreter_cmd = interpreter + ' ' + interpreter_args
        self.host = host
        if user != '':
            self.host = user + '@' + self.host
        self.queue_limit = queue_limit

        self.qsub_base_args = 'qsub -b y -wd ' + self.cluster_wd + ' ' + ge_aux_args
        if ge_gpu >= 0:
            self.qsub_base_args += ' -l gpu=' + str(ge_gpu)

        # create cluster wd if necessary
        cmd = 'mkdir -p ' + self.cluster_wd
        proc = subprocess.Popen(select_shell(cmd.split(), self.host))
        proc.wait()

    def sync(self, local_path, cluster_path, sync_to, exclude=[], recursive=False):
        exclude += self.exclude
        rsync.sync_folder(local_path, cluster_path, sync_to, exclude=exclude, remote_host=self.host, recursive=recursive)

    def submit_job(self, task_name, args, log_folder="./"):
        log_folder = enforce_trailing_backslash(log_folder)
        task_args = ' -N ' + task_name + \
                    ' -o ' + log_folder + task_name + '.log ' + \
                    ' -e ' + log_folder + task_name + '.error '

        qsub_str = self.qsub_base_args + task_args + self.interpreter_cmd
        qsub_cmd = qsub_str.split() + args

        proc = subprocess.Popen(select_shell(qsub_cmd, self.host), stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()

        out = stdout.split()
        ge_process_id = int(out[2])
        return ge_process_id

    def queue_state(self, job=None):
        proc = subprocess.Popen(select_shell(['qstat'], self.host), stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        lines = stdout.split(b'\n')

        queued = 0
        running = 0
        other = 0
        for line in lines[2:-1]:
            taskid = int(line.split()[0])
            if job == None or (taskid in [task.sge_job_id for task in job.tasks]):
                task_state = line.split()[4]
                if task_state == b'qw':
                    queued += 1
                elif task_state == b'r':
                    running += 1
                else:
                    other += 1

        return queued, running, other

    def is_job_finished(self, sge_job_id):
        proc = subprocess.Popen(select_shell(['qstat'], self.host), stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        lines = stdout.split(b'\n')

        for line in lines[2:-1]:
            if int(line.split()[0]) == sge_job_id:
                return False
        return True

    def queue_slots_available(self):
        queued, running, error = self.queue_state()
        return self.queue_limit - queued


class Local(Queue):
    def sync(self, local_path, cluster_path, sync_to, exclude=[], recursive=True):
        print('Local: no need to sync')

    def queue_slots_available(self):
        return 1

    def queue_state(self, job=None):
        return 0,0,0

    def submit_job(self, task_name, args, log_folder="./"):
        function_caller.main(args[1:])
        return 1

    def is_job_finished(self, sge_job_id):
        return True