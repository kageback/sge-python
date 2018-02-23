import subprocess
import os
import gridengine.rsync as rsync


class SGEEnvironment:

    def __init__(self, cluster_wd='~/runtime/env/', output_folder='out', data_folder='data', interpreter='python3', interpreter_args='-u',
                 ge_gpu=-1, ge_aux_args='', remote='', queue_limit=1):

        self.local_wd = './'
        self.cluster_wd = os.path.expanduser(cluster_wd) if remote == '' else cluster_wd
        self.cluster_wd = self.enforce_trailing_backslash(self.cluster_wd)

        self.output_folder = self.enforce_trailing_backslash(output_folder)
        self.data_folder = self.enforce_trailing_backslash(data_folder)

        self.interpreter_cmd = interpreter + ' ' + interpreter_args
        self.remote = remote
        self.queue_limit = queue_limit


        self.qsub_base_args = 'qsub -b y -wd ' + self.cluster_wd + ' ' + ge_aux_args

        if ge_gpu >= 0:
            self.qsub_base_args += ' -l gpu=' + str(ge_gpu)

        self.sge_jobids = []

    def enforce_trailing_backslash(self, path):
        if len(path) > 0 and path[-1] != '/':
            path += '/'
        return path

    def sync_to_cluster(self, from_path, to_path, skip_folders=['__pycache__', '.git', '.idea']):
        if os.path.isfile(from_path):
            pass
        elif os.path.isdir(from_path):
            rsync.sync_folder(from_path, to_path, skip_folders=skip_folders, remote_host=self.remote)
        else:
            raise IOError('rsync error. No such file or folder', from_path)


    def sync_results(self):
        cmd = self.base_rsync_cmd

        if self.remote != '':
            cmd += '-e ssh ' + self.remote + ':'

        cmd += self.cluster_wd + self.output_folder + ' ' + self.local_wd + self.output_folder

        proc = subprocess.Popen(cmd.split())
        proc.wait()

    def ssh_remote(self, cmd_list):
        if self.remote == '':
            return cmd_list

        # ssh shell no like \n so let's get rid of that kind of stuff
        for i in range(len(cmd_list)):
            cmd_list[i] = cmd_list[i].strip()

        cmd = 'ssh ' + self.remote + ' bash --login -c "' + ' '.join(cmd_list) + '"'

        return cmd.split()

    def submit_job(self, task_name, args):
        task_args = ' -N ' + task_name + \
                    ' -o ' + self.output_folder + task_name + '.log' + \
                    ' -e ' + self.output_folder + task_name + '.error '

        qsub_str = self.qsub_base_args + task_args + self.interpreter_cmd
        qsub_cmd = qsub_str.split() + args

        proc = subprocess.Popen(self.ssh_remote(qsub_cmd), stdout=subprocess.PIPE)#, cwd=self.wd)
        stdout, stderr = proc.communicate()

        out = stdout.split()
        ge_process_id = int(out[2])
        self.sge_jobids.append(ge_process_id)
        return ge_process_id

    def queue_state(self):
        proc = subprocess.Popen(self.ssh_remote(['qstat']), stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        lines = stdout.split(b'\n')

        queued = 0
        running = 0
        error = 0
        for line in lines[2:-1]:
            taskid = int(line.split()[0])
            if taskid in self.sge_jobids:
                task_state = line.split()[4]
                if task_state == b'qw':
                    queued += 1
                elif task_state == b'r':
                    running += 1
                else:
                    error += 1

        return queued, running, error

    def queue_slots_available(self):
        queued, running, error = self.queue_state()
        return self.queue_limit - queued
