import subprocess
import os
import time
import pickle
import codecs

class Job:
    def __init__(self, jobs_path='jobs', job_id='job', load_existing_job=False):
        self.output_dir = os.path.dirname(jobs_path + '/')
        self.last_task_id = -1
        self.ge_job_ids = {}

        if load_existing_job:
            self.job_id = job_id
            self.job_dir = self.output_dir + '/' + self.job_id

            if not os.path.isdir(self.job_dir):
                raise ImportError('Job missing! ', self.job_dir)

        else: # Create new job (and if nessasary jobs directory)
            if not os.path.isdir(self.output_dir):
                print('Jobs directory missing. Creating directory: ' + os.path.realpath(self.output_dir))
                os.mkdir(self.output_dir)

            i = 0
            while(True):
                self.job_id = job_id + '.' + str(i)
                self.job_dir = self.output_dir + '/' + self.job_id

                if os.path.isdir(self.job_dir):
                    i += 1
                else:
                    os.mkdir(self.job_dir)
                    break

    def run_python_script(self, script_path, interpreter='python3', interpreter_args='-u', ge_cwd=True, ge_gpu=-1, ge_aux_args='', **kwargs):
        self.last_task_id += 1
        task_name = get_task_name(self.last_task_id)
        ge_args = '-b y' + ' -N ' + self.job_id + '.' + str(self.last_task_id) + \
                  ' -o ' + self.job_dir + '/' + task_name + '.log' + \
                  ' -e ' + self.job_dir + '/' + task_name + '.error ' + \
                  ge_aux_args
        if ge_cwd:
            ge_args += ' -cwd'
        if ge_gpu >= 0:
            ge_args += ' -l gpu=' + str(ge_gpu)

        script_args = ' --save_path "' + self.job_dir + '" --exp_name "' + task_name + '"'
        for arg, val in kwargs.items():
            script_args += ' --'+ arg + ' ' + str(val)

        qsub_args = ge_args + ' ' +  interpreter + ' ' + interpreter_args + ' ' + script_path + script_args

        self.ge_job_ids[self.last_task_id] = qsub(qsub_args)

        return self.last_task_id

    def run_python_func(self, f, *args, interpreter='python3', **kwargs):
        module_name = f.__module__
        func_name = f.__name__

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        self.last_task_id += 1
        task_name = get_task_name(self.last_task_id)
        result_path = self.job_dir + '/' + task_name + '.func_res.pkl'

        pickled_kwargs = codecs.encode(pickle.dumps(kwargs), "base64").decode()
        pickled_args = codecs.encode(pickle.dumps(args), "base64").decode()

        proc = subprocess.Popen([interpreter,
                                 self_relative_project_path + '/function_caller.py',
                                 module_name,
                                 func_name,
                                 result_path,
                                 pickled_args,
                                 pickled_kwargs], stdout=subprocess.PIPE)

        stdout, stderr = proc.communicate()
        log = stdout.decode("utf-8")
        pickled_res = stdout.split(b'\n')[-2]
        res = pickle.loads(codecs.decode(pickled_res, "base64"))
        return res, log  # stdout.decode("utf-8")

    def wait(self):
        done = False
        while(not done):

            proc = subprocess.Popen('qstat', stdout=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            lines = stdout.split(b'\n')

            running = 0
            for line in lines[2:-1]:
                if int(line.split()[0]) in self.ge_job_ids.values():
                    running += 1
            if running > 0:
                print('\rwaiting for ' + str(running) + ' of ' + str(self.last_task_id+1) + ' tasks to fininsh', end="")
                time.sleep(1)
            else:
                print('All tasks completed!')
                done = True



    def erase_job(self):
        os.rmdir(self.job_dir)


def qsub(args):
    cmd = 'qsub ' + args
    print('running command:',cmd)
    proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    proc.wait()

    out = proc.stdout.readline()
    out = out.split()
    ge_process_id = int(out[2])

    return ge_process_id

def get_task_name(task_id):
    return 'task.' + str(task_id)
