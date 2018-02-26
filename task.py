import os
import pickle
import time
from gridengine.misc import *

def save(task):
    with open(task.output_folder + task.task_name + ".pkl", 'wb') as f:
        pickle.dump(task, f)


def load(task_name, output_folder, job_folder='jobs'):
    job_folder =  enforce_trailing_backslash(job_folder)
    output_folder = enforce_trailing_backslash(output_folder)
    with open(job_folder + output_folder + task_name + ".pkl", 'rb') as f:
        return pickle.load(f)

class Task:
    def __init__(self, comp_env, task_name="task", output_folder="./",):
        self.task_name = task_name
        self.comp_env = comp_env
        self.function = None
        self.output_folder = enforce_trailing_backslash(output_folder)
        self.result_path = output_folder + task_name + '.func_res.pkl'
        self.args_path = output_folder + task_name + '.func_args.pkl'

    def run_function(self, f, args, kwargs):
        self.function = f
        save(self)

        module_name = f.__module__
        func_name = f.__name__

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        with open(self.args_path, 'wb') as f:
            pickle.dump([args, kwargs], f)

        self.comp_env.sync_to_cluster(self.comp_env.local_wd   + self.output_folder,
                                      self.comp_env.cluster_wd + self.output_folder)

        self.comp_env.submit_job(self.task_name, [self_relative_project_path + '/function_caller.py',
                                                  module_name,
                                                  func_name,
                                                  self.args_path,
                                                  self.result_path],
                                                 self.output_folder)

    def get_result(self, wait=True):
        task_res = None
        while wait and task_res is None:
            if not os.path.isfile(self.result_path):
                self.comp_env.sync_from_cluster(self.comp_env.local_wd + self.output_folder,
                                                self.comp_env.cluster_wd + self.output_folder)
            try:
                with open(self.result_path, 'rb') as f:
                    task_res = pickle.load(f)
            except FileNotFoundError:
                time.sleep(10)

        return task_res
