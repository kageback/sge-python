import subprocess
import os

class BaseEnviroment:
    def __init__(self, runtime_path='~/runtime/env/', interpreter='python3', interpreter_args='-u'):
        self.wd = os.path.expanduser(runtime_path)
        if len(self.wd) > 0 and self.wd[-1] != '/':
            self.wd += '/'
        self.interpreter_cmd = interpreter + ' ' + interpreter_args

    def submit_job(self, job, args):
        pass

    def sync_code(self):
        if self.wd != '':
            cmd = 'rsync -r --exclude __pycache__ --exclude .git --exclude .idea ./ ' + self.wd
            subprocess.Popen(cmd.split())

    def sync_results(self):
        if self.wd != '':
            cmd = 'rsync -r --exclude __pycache__ --exclude .git --exclude .idea ' + self.wd + ' .'
            subprocess.Popen(cmd.split())


class GECluster(BaseEnviroment):
    def __init__(self, runtime_path='~/runtime/env/', interpreter='python3', interpreter_args='-u', ge_gpu=-1, ge_aux_args=''):
        super().__init__(runtime_path, interpreter, interpreter_args)
        self.qsub_base_args = 'qsub -cwd -b y ' + ge_aux_args

        if ge_gpu >= 0:
            self.qsub_base_args += ' -l gpu=' + str(ge_gpu)

    def submit_job(self, job, args):
        self.sync_code()

        qsub_str = self.qsub_base_args + self.task_args(job) + self.interpreter_cmd
        qsub_cmd = qsub_str.split() + args

        proc = subprocess.Popen(qsub_cmd, stdout=subprocess.PIPE, cwd=self.wd)
        stdout, stderr = proc.communicate()

        out = stdout.split()
        ge_process_id = int(out[2])
        return ge_process_id

    def task_args(self, job):
         return ' -N ' + job.job_id + '.' + str(job.last_task_id) + \
                ' -o ' + job.job_dir + '/' + job.get_task_name(job.last_task_id) + '.log' + \
                ' -e ' + job.job_dir + '/' + job.get_task_name(job.last_task_id) + '.error '


class LocalProcess(BaseEnviroment):
    def __init__(self, runtime_path='~/runtime/env/', interpreter='python3', interpreter_args='-u'):
        super().__init__(runtime_path, interpreter, interpreter_args)

    def submit_job(self, job, args):
        self.sync_code()

        cmd = self.interpreter_cmd.split() + args

        proc = subprocess.Popen(cmd)
        # maybe pipe logs to file?
        #stdout, stderr = proc.communicate()
        #log = stdout.decode("utf-8")


        return proc.pid  # stdout.decode("utf-8")
