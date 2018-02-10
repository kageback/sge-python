import subprocess
import os
import time
import pickle
import codecs

from gridengine.compute_enviroment import SGEEnvironment


class Job:
    def __init__(self, jobs_path='jobs', job_id='job', load_existing_job=False, compute_environments=[]):
        self.compute_environments = compute_environments
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

    def erase_job(self):
        os.rmdir(self.job_dir)

    def run_function(self, f, *args, **kwargs):
        module_name = f.__module__
        func_name = f.__name__
        comp_env = self.get_free_env()

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        task_id = self.get_next_task_id()

        result_path = self.get_result_path(task_id)

        pickled_kwargs = codecs.encode(pickle.dumps(kwargs), "base64").decode()
        pickled_args = codecs.encode(pickle.dumps(args), "base64").decode()

        self.ge_job_ids[self.last_task_id] = comp_env.submit_job(self, [self_relative_project_path + '/function_caller.py',
                                                                       module_name,
                                                                       func_name,
                                                                       result_path,
                                                                       pickled_args,
                                                                       pickled_kwargs])
        return self.last_task_id

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

    def get_result(self, task_id, wait=False):
        result_path = self.get_result_path(task_id)

        task_res = None
        while wait and task_res is None:
            try:
                self.compute_environments[0].sync_results()
                with open(result_path, 'rb') as f:
                    task_res = pickle.load(f)
            except FileNotFoundError:
                time.sleep(1)

        return task_res

    def get_free_env(self):
        # This func needs to be more clever to handle multiple environments
        return self.compute_environments[0]

    def get_next_task_id(self):
        self.last_task_id += 1
        return self.last_task_id

    def get_result_path(self, task_id):
        task_name = self.get_task_name(task_id)
        result_path = self.job_dir + '/' + task_name + '.func_res.pkl'
        return result_path

    def get_task_name(self, task_id):
        return 'task.' + str(task_id)

