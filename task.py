import os
import pickle as pickle
import time
from gridengine.misc import *
from gridengine import SyncTo
from gridengine.result_wrapper import ResultWrapper

def save(task):
    with open(task.task_path, 'wb') as f:
        pickle.dump(task, f)


def load(task_name, output_folder, job_folder='jobs'):
    job_folder = enforce_trailing_backslash(job_folder)
    output_folder = enforce_trailing_backslash(output_folder)
    with open(job_folder + output_folder + task_name + ".pkl", 'rb') as f:
        return pickle.load(f)

class Task:
    def __init__(self, f, args, kwargs, task_name="task", output_folder="./", dependencies=[]):
        self.function = f
        self.args = args
        self.kwargs = kwargs

        self.dependencies = dependencies

        self.task_name = task_name

        self.output_folder = enforce_trailing_backslash(output_folder)
        self.task_path = output_folder + task_name + ".task.pkl"
        self.result_base_path = output_folder + task_name + ".result"

        self.queue = None
        self.sge_job_id = None

        #self.result = Results(self.result_base_path)
        self.results = {}

    def result(self, result_index=0):
        if result_index in self.results:
            return self.results[result_index]

        self.results[result_index] = ResultWrapper(self.result_base_path + '('+str(result_index) + ')', self.sge_job_id)
        return self.results[result_index]

    def schedule(self, queue):

        self.queue = queue
        save(self)

        self.queue.sync(self.queue.local_wd + self.output_folder,
                        self.output_folder,
                        SyncTo.REMOTE, recursive=True)

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        self.sge_job_id = self.queue.submit_job(self.task_name,
                                                [self_relative_project_path + '/function_caller.py', self.task_path],
                                                self.output_folder,
                                                dependencies=self.dependencies)


    def get_result(self, wait=True, retry_interval=1):
        # todo remove?
        if self.queue is None:
            return None

        task_res = None
        while task_res is None:
            # sync results folder
            if not os.path.isfile(self.result_base_path):
                self.queue.sync(self.queue.local_wd + self.output_folder,
                                self.output_folder,
                                SyncTo.LOCAL, recursive=True)

            # Read if available.
            if os.path.isfile(self.result_base_path):
                try:
                    with open(self.result_base_path, 'rb') as f:
                        task_res = pickle.load(f)
                except EOFError:
                    print('downloading result...')
                    time.sleep(retry_interval)
                    self.queue.sync(self.queue.local_wd + self.output_folder,
                                    self.output_folder,
                                    SyncTo.LOCAL, recursive=True)
            elif wait:
                time.sleep(retry_interval)
            else:
                return None

        return task_res
