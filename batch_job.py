import os
import time
import pickle

from gridengine.task import Task

def save(job):
    with open(job.job_dir + "job.pkl", 'wb') as f:
        pickle.dump(job, f)


def load(job_id, jobs_base_path='jobs'):
    jobs_base_path = os.path.dirname(jobs_base_path + '/') + '/'
    save_path = jobs_base_path + job_id + "/job.pkl"
    with open(save_path, 'rb') as f:
        return pickle.load(f)


class Job:
    def __init__(self, jobs_base_path='jobs', job_id_prefix='job'):

        jobs_base_path = os.path.dirname(jobs_base_path + '/') + '/'

        self.compute_environments = []
        self.tasks = []

        # Create job directory
        if not os.path.isdir(jobs_base_path):
            print('Jobs directory missing. Creating directory: ' + os.path.realpath(jobs_base_path))
            os.mkdir(jobs_base_path)
        i = 0
        while(True):
            self.job_name = job_id_prefix + '.' + str(i)
            self.job_dir = jobs_base_path + self.job_name + '/'

            if os.path.isdir(self.job_dir):
                i += 1
            else:
                os.mkdir(self.job_dir)
                break

    def add_compute_environment(self, env):
        self.compute_environments.append(env)

    def run_function(self, f, *args, **kwargs):

        comp_env = self.get_free_env()

        task_name = self.job_name + '.' + str(len(self.tasks))
        task = Task(comp_env, task_name, self.job_dir)
        self.tasks.append(task)

        task.run_function(f, args, kwargs)

        return task

    def rerun_task(self, f, task_id):
        module_name = f.__module__
        func_name = f.__name__
        comp_env = self.get_free_env()

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        args_path = self.get_args_path(task_id)
        result_path = self.get_result_path(task_id)

        self.ge_job_ids[self.last_task_id] = comp_env.submit_job(self.get_task_name(task_id),
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


if __name__ == "__main__":
    # Test/demo code


    job = Job()
    jobname = job.job_name

    save(job)

    job = load(jobname)

    print(job)

