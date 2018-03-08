import os
import time
import pickle as pickle
from functools import reduce

from gridengine.task import Task
from gridengine.result_wrapper import ResultWrapper


class Job:
    def __init__(self, queue, jobs_base_path='jobs', job_id_prefix='job'):
        self.queue = queue

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

    def run(self, f, *args, **kwargs):
        dependencies = self.get_job_dependencies(args, kwargs)

        task_name = self.job_name + '.' + str(len(self.tasks))

        task = Task(f, args, kwargs, task_name, self.job_dir, dependencies)
        self.tasks.append(task)

        task.schedule(self.queue)


        return task

    def get_job_dependencies(self, args, kwargs):
        dependencies = []
        for i in range(len(args)):
            if isinstance(args[i], ResultWrapper):
                dependencies.append(args[i].sge_job_id)
        for k in kwargs:
            if isinstance(kwargs[k], ResultWrapper):
                dependencies.append(kwargs[k].sge_job_id)
        return dependencies

    def wait(self, retry_interval=5):
        done = False
        while(not done):

            states = self.queue.queue_stat(sge_job_ids=[t.sge_job_id for t in self.tasks])
            if len(states) == 0:
                print('\nAll tasks completed!')
                done = True
            else:
                print('\rWaiting for ' + str(reduce((lambda x, y: x + y), states.values())) + ' tasks in states: ' + str(states) , end="")
                time.sleep(retry_interval)

    def save(self):
        with open(self.job_dir + "job.pkl", 'wb') as f:
            pickle.dump(self, f)

    @staticmethod
    def load(job_id, jobs_base_path='jobs'):
        jobs_base_path = os.path.dirname(jobs_base_path + '/') + '/'
        save_path = jobs_base_path + job_id + "/job.pkl"
        with open(save_path, 'rb') as f:
            return pickle.load(f)


