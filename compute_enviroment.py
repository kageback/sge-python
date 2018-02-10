import subprocess
import os


class BaseEnvironment:

    def __init__(self, runtime_path='~/runtime/env/', interpreter='python3', interpreter_args='-u', remote=''):

        self.wd = os.path.expanduser(runtime_path) if remote == '' else runtime_path
        if len(self.wd) > 0 and self.wd[-1] != '/':
            self.wd += '/'

        self.interpreter_cmd = interpreter + ' ' + interpreter_args
        self.remote = remote

        self.base_rsync_cmd = 'rsync -r --exclude __pycache__ --exclude .git --exclude .idea '

    def submit_job(self, job, args):
        self.sync_code()

    def sync_code(self):
        if self.remote != '':
            cmd = self.base_rsync_cmd + '-e ssh ' + './ ' + self.remote + ':' + self.wd
        else:
            cmd = self.base_rsync_cmd + './ ' + self.wd

        subprocess.Popen(cmd.split())

    def sync_results(self):
        if self.remote != '':
            cmd = self.base_rsync_cmd + '-e ssh ' + self.remote + ':' + self.wd + ' .'
        else:
            cmd = self.base_rsync_cmd + self.wd + ' .'

        subprocess.Popen(cmd.split())

    def ssh_remote(self, cmd_list):
        if self.remote == '':
            return cmd_list

        # ssh command no like \n so let's get rid of that kind of stuff
        for i in range(len(cmd_list)):
            cmd_list[i] = cmd_list[i].strip()

        cmd = 'ssh ' + self.remote + ' bash --login -c "' + ' '.join(cmd_list) + '"'

        return cmd.split()


class SGEEnvironment(BaseEnvironment):

    def __init__(self, runtime_path='~/runtime/env/', interpreter='python3', interpreter_args='-u', ge_gpu=-1, ge_aux_args='', remote=''):
        super().__init__(runtime_path, interpreter, interpreter_args, remote=remote)



        self.qsub_base_args = 'qsub -b y -wd ' + self.wd + ' ' + ge_aux_args

        if ge_gpu >= 0:
            self.qsub_base_args += ' -l gpu=' + str(ge_gpu)



    def submit_job(self, job, args):
        super(SGEEnvironment, self).submit_job(job, args)


        qsub_str = self.qsub_base_args + self.task_args(job) + self.interpreter_cmd
        qsub_cmd = qsub_str.split() + args

        proc = subprocess.Popen(self.ssh_remote(qsub_cmd), stdout=subprocess.PIPE)#, cwd=self.wd)
        stdout, stderr = proc.communicate()

        out = stdout.split()
        ge_process_id = int(out[2])
        return ge_process_id

    def task_args(self, job):
         return ' -N ' + job.job_id + '.' + str(job.last_task_id) + \
                ' -o ' + job.job_dir + '/' + job.get_task_name(job.last_task_id) + '.log' + \
                ' -e ' + job.job_dir + '/' + job.get_task_name(job.last_task_id) + '.error '


class SubProcessEnvironment(BaseEnvironment):

    def __init__(self, runtime_path='~/runtime/env/', interpreter='python3', interpreter_args='-u'):
        super().__init__(runtime_path, interpreter, interpreter_args)

    def submit_job(self, job, args):
        super(SubProcessEnvironment, self).submit_job(job, args)

        cmd = self.interpreter_cmd.split() + args

        proc = subprocess.Popen(cmd, cwd=self.wd)

        return proc.pid
