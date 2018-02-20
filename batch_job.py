import subprocess
import os
import time
import pickle
import codecs

from gridengine.compute_enviroment import SGEEnvironment


class Job:
    def __init__(self, jobs_path='jobs', job_id='job', load_existing_job=False):
        self.compute_environments = []
        self.output_dir = os.path.dirname(jobs_path + '/')
        self.last_task_id = -1
        self.ge_job_ids = {}
        self.task_env = {}

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

    def add_compute_environment(self, env):
        self.compute_environments.append(env)

    def erase_job(self):
        os.rmdir(self.job_dir)

    def run_function(self, f, *args, **kwargs):
        module_name = f.__module__
        func_name = f.__name__
        comp_env = self.get_free_env()

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        task_id = self.get_next_task_id()

        args_path = self.get_args_path(task_id)
        result_path = self.get_result_path(task_id)

        with open(args_path, 'wb') as f:
            pickle.dump([args, kwargs], f)


        self.ge_job_ids[self.last_task_id] = comp_env.submit_job(self.get_task_name(self.last_task_id),
                                                                 [self_relative_project_path + '/function_caller.py',
                                                                 module_name,
                                                                 func_name,
                                                                 args_path,
                                                                 result_path])

        self.task_env[self.last_task_id] = comp_env

        return self.last_task_id

    def rerun_task(self, f, task_id):
        module_name = f.__module__
        func_name = f.__name__
        comp_env = self.get_free_env()

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        args_path = self.get_args_path(task_id)
        result_path = self.get_result_path(task_id)

        self.ge_job_ids[self.last_task_id] = comp_env.submit_job(self.get_task_name(self.last_task_id),
                                                                 [self_relative_project_path + '/function_caller.py',
                                                                 module_name,
                                                                 func_name,
                                                                 args_path,
                                                                 result_path])

    def wait(self):
        done = False
        while(not done):

            queued = 0
            running = 0
            error = 0
            for env in self.compute_environments:
                q, r, e = env.queue_state()
                queued += q
                running += r
                error += e

            if queued + running > 0:
                print('\rWaiting for ' + str(running) + ' running and ' + str(queued) + ' queued tasks to finish', end="")
                time.sleep(10)
            elif error > 0:
                print('\nAll tasks terminated! ' + str(error) + ' tasks stuck in error state')
                done = True
            else:
                print('\nAll tasks completed!')
                done = True

    def get_args(self, task_id):
        args_path = self.get_args_path(task_id)
        try:
            with open(args_path, 'rb') as f:
                arguments = pickle.load(f)
            args = arguments[0]
            kwargs = arguments[1]
            return args, kwargs
        except FileNotFoundError:
            return None, None

    def get_result(self, task_id, wait=True):
        result_path = self.get_result_path(task_id)
        task_res = None
        while wait and task_res is None:
            if not os.path.isfile(result_path):
                self.task_env[task_id].sync_results()
            try:
                with open(result_path, 'rb') as f:
                    task_res = pickle.load(f)
            except FileNotFoundError:
                time.sleep(60)

        return task_res

    def get_free_env(self, wait=True):

        free_env = None
        max_free_queue_slots = 0
        while wait and free_env is None:
            for env in self.compute_environments:
                free_queue_slots = env.queue_slots_available()
                if free_queue_slots > max_free_queue_slots:
                    max_free_queue_slots = free_queue_slots
                    free_env = env
            if free_env is None:
                #print('\rWaiting for queue slot... ', end="")
                time.sleep(1)
        #print("Queued slot assigned.")

        return free_env

    def get_next_task_id(self):
        self.last_task_id += 1
        return self.last_task_id

    def get_result_path(self, task_id):
        task_name = self.get_task_name(task_id)
        result_path = self.job_dir + '/' + task_name + '.func_res.pkl'
        return result_path

    def get_args_path(self, task_id):
        task_name = self.get_task_name(task_id)
        result_path = self.job_dir + '/' + task_name + '.func_args.pkl'
        return result_path


    def get_task_name(self, task_id):
        return str(self.job_id) + '.' + str(task_id)

