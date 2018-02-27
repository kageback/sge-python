import os
import time
import dill as pickle

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

    def run_function(self, comp_env, f, args, kwargs):

        task_name = self.job_name + '.' + str(len(self.tasks))

        task = Task(f, args, kwargs, task_name, self.job_dir)
        self.tasks.append(task)

        task.schedule(comp_env)

        return task



