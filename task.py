import os
import pickle as pickle
import time
from gridengine.misc import *
from gridengine import SyncTo

def save(task):
    with open(task.task_path, 'wb') as f:
        pickle.dump(task, f)


def load(task_name, output_folder, job_folder='jobs'):
    job_folder = enforce_trailing_backslash(job_folder)
    output_folder = enforce_trailing_backslash(output_folder)
    with open(job_folder + output_folder + task_name + ".pkl", 'rb') as f:
        return pickle.load(f)

class Task:
    def __init__(self, f, args, kwargs, task_name="task", output_folder="./",):
        self.function = f
        self.args = args
        self.kwargs = kwargs

        self.task_name = task_name

        self.output_folder = enforce_trailing_backslash(output_folder)
        self.task_path = output_folder + task_name + ".task.pkl"
        self.result_path = output_folder + task_name + ".result.pkl"

        self.queue = None
        self.ge_jobid = None

    def schedule(self, queue):
        self.queue = queue
        save(self)

        self.queue.sync(self.queue.local_wd + self.output_folder,
                        self.queue.cluster_wd + self.output_folder,
                        SyncTo.REMOTE)

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        self.ge_jobid = self.queue.submit_job(self.task_name,
                                              [self_relative_project_path + '/function_caller.py', self.task_path],
                                              self.output_folder)

    def get_result(self, wait=True, retry_interval=1):
        if self.queue is None:
            return None

        task_res = None
        while task_res is None:
            # sync results folder
            if not os.path.isfile(self.result_path):
                self.queue.sync(self.queue.local_wd + self.output_folder,
                                self.queue.cluster_wd + self.output_folder,
                                SyncTo.LOCAL)

            # Read if available.
            if os.path.isfile(self.result_path):
                try:
                    with open(self.result_path, 'rb') as f:
                        task_res = pickle.load(f)
                except EOFError:
                    print('downloading result...')
                    time.sleep(retry_interval)
                    self.queue.sync(self.queue.local_wd + self.output_folder,
                                    self.queue.cluster_wd + self.output_folder,
                                    SyncTo.LOCAL)
            elif wait:
                time.sleep(retry_interval)
            else:
                return None

        return task_res


class WaitTask:
    def __init__(self, task_name='wait_task'):
        self.ge_jobid = 0
        self.task_name = task_name
