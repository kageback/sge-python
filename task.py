import os
import dill as pickle
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
        task_res = None
        retry = False
        while wait and task_res is None:
            try:
                with open(self.result_path, 'rb') as f:
                    task_res = pickle.load(f)
            except FileNotFoundError:
                if retry:
                    time.sleep(retry_interval)
                else:
                    retry = True

                self.queue.sync(self.queue.local_wd + self.output_folder,
                                self.queue.cluster_wd + self.output_folder,
                                SyncTo.LOCAL)

        return task_res
