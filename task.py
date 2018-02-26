import os
import pickle
import time
from gridengine.misc import *

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
        self.task_path = output_folder + task_name + ".pkl"

        self.comp_env = None
        self.result = None

        save(self)

    def schedule(self, comp_env):
        self.comp_env = comp_env

        self.comp_env.sync_to_cluster(self.comp_env.local_wd + self.output_folder,
                                      self.comp_env.cluster_wd + self.output_folder)

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        self.comp_env.submit_job(self.task_name,
                                 [self_relative_project_path + '/function_caller.py', self.task_path],
                                 self.output_folder)

    def get_result(self, wait=True, retry_interval=5):
        task_res = None
        retry = False
        while wait and task_res is None:
            with open(self.task_path, 'rb') as f:
                task = pickle.load(f)
            task_res = task.result
            if task_res is None:
                if retry:
                    time.sleep(retry_interval)
                else:
                    retry = True

                self.comp_env.sync_from_cluster(self.comp_env.local_wd + self.output_folder,
                                                self.comp_env.cluster_wd + self.output_folder)

        return task_res
