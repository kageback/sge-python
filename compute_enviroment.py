import subprocess
import os


class SGEEnvironment:

    def __init__(self, runtime_path='~/runtime/env/', output_folder='out', data_folder='data', interpreter='python3', interpreter_args='-u',
                 ge_gpu=-1, ge_aux_args='', remote='', queue_limit=1):

        self.local_wd = './'
        self.wd = os.path.expanduser(runtime_path) if remote == '' else runtime_path
        self.wd = self.enforce_trailing_backslash(self.wd)

        self.output_folder = self.enforce_trailing_backslash(output_folder)
        self.data_folder = self.enforce_trailing_backslash(data_folder)

        self.interpreter_cmd = interpreter + ' ' + interpreter_args
        self.remote = remote
        self.queue_limit = queue_limit

        self.base_rsync_cmd = 'rsync -vr --exclude __pycache__ --exclude .git --exclude .idea --exclude ' + data_folder + ' '

        self.qsub_base_args = 'qsub -b y -wd ' + self.wd + ' ' + ge_aux_args

        if ge_gpu >= 0:
            self.qsub_base_args += ' -l gpu=' + str(ge_gpu)

        self.tasks = []

    def enforce_trailing_backslash(self, path):
        if len(path) > 0 and path[-1] != '/':
            path += '/'
        return path

    def sync_data(self):
        cmd = 'rsync -vr --exclude __pycache__ '

        if self.remote != '':
            cmd = self.base_rsync_cmd + '-e ssh ' + self.local_wd + self.data_folder + ' ' + self.remote + ':' + self.wd + self.data_folder
        else:
            cmd = self.base_rsync_cmd + self.local_wd + self.data_folde + ' ' + self.wd + self.data_folde

        proc = subprocess.Popen(cmd.split())
        proc.wait()

    def sync_code(self):
        if self.remote != '':
            cmd = self.base_rsync_cmd + '-e ssh ' + self.local_wd + ' ' + self.remote + ':' + self.wd
        else:
            cmd = self.base_rsync_cmd + self.local_wd + ' ' + self.wd

        proc = subprocess.Popen(cmd.split())
        proc.wait()

    def sync_results(self):
        cmd = self.base_rsync_cmd

        if self.remote != '':
            cmd += '-e ssh ' + self.remote + ':'

        cmd += self.wd + self.output_folder + ' ' + self.local_wd + self.output_folder

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
        self.sync_code()

        task_args = ' -N ' + task_name + \
                    ' -o ' + self.output_folder + task_name + '.log' + \
                    ' -e ' + self.output_folder + task_name + '.error '

        qsub_str = self.qsub_base_args + task_args + self.interpreter_cmd
        qsub_cmd = qsub_str.split() + args

        proc = subprocess.Popen(self.ssh_remote(qsub_cmd), stdout=subprocess.PIPE)#, cwd=self.wd)
        stdout, stderr = proc.communicate()

        out = stdout.split()
        ge_process_id = int(out[2])
        self.tasks.append(ge_process_id)
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
            if taskid in self.tasks:
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


class SubProcessEnvironment(SGEEnvironment):

    def __init__(self, runtime_path='~/runtime/env/', interpreter='python3', interpreter_args='-u'):
        super().__init__(runtime_path, interpreter, interpreter_args)

    def submit_job(self, job, args):
        super(SubProcessEnvironment, self).submit_job(job, args)

        cmd = self.interpreter_cmd.split() + args

        proc = subprocess.Popen(cmd, cwd=self.wd)

        return proc.pid
