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

        task = Task(f, args, kwargs, task_name, self.job_dir)
        self.tasks.append(task)

        task.schedule(comp_env)

        return task

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
                time.sleep(5)
        return free_env

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



