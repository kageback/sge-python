import os
import pickle as pickle
import time
from expgraph.misc import *
from expgraph.rsync import SyncTo
from expgraph.result_wrapper import ResultWrapper

def save(task):
    with open(task.task_path, 'wb') as f:
        pickle.dump(task, f)


def load(task_name, task_folder, job_folder='jobs'):
    job_folder = enforce_trailing_backslash(job_folder)
    task_folder = enforce_trailing_backslash(task_folder)
    with open(job_folder + task_folder + task_name + ".pkl", 'rb') as f:
        return pickle.load(f)

class Task:
    def __init__(self, f, args, kwargs, task_name="task", task_folder="./", dependencies=[]):
        self.function = f
        self.args = list(args)
        self.kwargs = kwargs

        self.dependencies = dependencies

        self.task_name = task_name

        self.task_folder = enforce_trailing_backslash(task_folder)

        self.task_path = self.task_folder + task_name + ".task.pkl"
        self.result_base_path = self.task_folder + task_name + ".result"

        self.queue = None
        self.sge_job_id = None

        self.results = {}

    def result(self, result_index=0):
        if result_index in self.results:
            return self.results[result_index]

        self.results[result_index] = ResultWrapper(self, self.result_base_path + '('+str(result_index) + ')')
        return self.results[result_index]

    def schedule(self, queue):

        self.queue = queue
        save(self)

        self.queue.sync(self.queue.local_wd + self.task_path,
                        self.task_folder,
                        SyncTo.REMOTE, recursive=False)

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        self.sge_job_id = self.queue.submit_job(self.task_name,
                                                [self_relative_project_path + '/function_caller.py', self.task_path],
                                                self.task_folder,
                                                dependencies=self.dependencies)

    def execute(self):
        # print func ref to log
        # print('function = ', task.function)

        # Print arguments to log
        # print('args =', task.args)
        # print('kwargs =', task.kwargs)

        # get wrapped results
        for i in range(len(self.args)):
            if isinstance(self.args[i], ResultWrapper):
                self.args[i] = self.args[i].get()
        for k in self.kwargs:
            if isinstance(self.kwargs[k], ResultWrapper):
                self.kwargs[k] = self.kwargs[k].get()

        # unpack member if attached to obj
        if 'call_member' in self.kwargs:
            if isinstance(self.function, Task):
                self.obj = self.function.result().get()
            try:
                self.function = getattr(self.obj, self.kwargs['call_member'])
                del self.kwargs['call_member']
            except AttributeError:
                print(self.kwargs['call_member'] + " not found")

        # call function
        result = self.function(*self.args, **self.kwargs)

        # Add results
        if type(result) is tuple:
            for res, i in zip(result, range(len(result))):
                self.result(i).set(res)
        else:
            self.result(0).set(result)


    def get_result(self, wait=True, retry_interval=1):
        # todo remove?
        if self.queue is None:
            return None

        task_res = None
        while task_res is None:
            # sync results folder
            if not os.path.isfile(self.result_base_path):
                self.queue.sync(self.queue.local_wd + self.task_folder,
                                self.task_folder,
                                SyncTo.LOCAL, recursive=True)

            # Read if available.
            if os.path.isfile(self.result_base_path):
                try:
                    with open(self.result_base_path, 'rb') as f:
                        task_res = pickle.load(f)
                except EOFError:
                    print('downloading result...')
                    time.sleep(retry_interval)
                    self.queue.sync(self.queue.local_wd + self.task_folder,
                                    self.task_folder,
                                    SyncTo.LOCAL, recursive=True)
            elif wait:
                time.sleep(retry_interval)
            else:
                return None

        return task_res
