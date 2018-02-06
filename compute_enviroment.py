import subprocess


class GECluster:
    def __init__(self, working_directory='./', interpreter='python3', interpreter_args='-u', ge_gpu=-1, ge_aux_args=''):
        self.wd = working_directory
        self.interpreter_cmd = interpreter + ' ' + interpreter_args

        self.qsub_base_args = 'qsub -cwd -b y ' + ge_aux_args

        if ge_gpu >= 0:
            self.qsub_base_args += ' -l gpu=' + str(ge_gpu)

    def submit_job(self, job, args):
        qsub_str = self.qsub_base_args + self.task_args(job) + self.interpreter_cmd
        qsub_cmd = qsub_str.split() + args

        proc = subprocess.Popen(qsub_cmd, stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()

        out = stdout.split()
        ge_process_id = int(out[2])
        return ge_process_id

    def task_args(self, job):
         return ' -N ' + job.job_id + '.' + str(job.last_task_id) + \
                ' -o ' + job.job_dir + '/' + job.get_task_name(job.last_task_id) + '.log' + \
                ' -e ' + job.job_dir + '/' + job.get_task_name(job.last_task_id) + '.error '


def qsub(args):
    cmd = 'qsub ' + args
    print('running command:', cmd)
    proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    proc.wait()

    out = proc.stdout.readline()
    out = out.split()
    ge_process_id = int(out[2])

    return ge_process_id