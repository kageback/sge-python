import pickle
import os
import time
from gridengine import SyncTo


class Results:
    def __init__(self, path):
        self.result_path = path
        self.results = {}

    def __call__(self, result_index=0):
        if result_index in self.results:
            return self.results[result_index]

        self.results[result_index] = ResultWrapper(self.result_path + '('+str(result_index) + ')')
        return self.results[result_index]

    def set_queue(self, queue, sge_job_id):
        for res_wrapper in self.results.values():
            res_wrapper.queue = queue
            res_wrapper.sge_job_id = sge_job_id


class ResultWrapper:
    def __init__(self, path):
        self.result_path = path
        self.queue = None
        self.sge_job_id = None

    def set(self, result):
        # Todo Save numpy, pytorch and pandas using custom serialization

        # save the results
        with open(self.result_path + '.pkl', 'wb') as f:
            pickle.dump(result, f)

    def get(self):
        # Handle when not on disk (task run on another node)
        with open(self.result_path + '.pkl', 'rb') as f:
            return pickle.load(f)

    def is_finished(self):
        if (self.queue is None)  or (self.sge_job_id is None):
            return False

        return self.queue.is_job_finished(self.sge_job_id)

    def fetch_result(self, wait=True, retry_interval=1):
        task_res = None
        while task_res is None:
            # sync results folder
            if not os.path.isfile(self.result_path):

                self.queue.sync(self.queue.local_wd + self.result_path,
                                self.queue.cluster_wd + self.result_path,
                                SyncTo.LOCAL, recursive=False)

            # Read if available.
            if os.path.isfile(self.result_path):
                try:
                    with open(self.result_path, 'rb') as f:
                        task_res = pickle.load(f)
                except EOFError:
                    print('downloading result...')
                    time.sleep(retry_interval)
                    self.queue.sync(self.queue.local_wd + self.result_path,
                                    self.queue.cluster_wd + self.result_path,
                                    SyncTo.LOCAL, recursive=False)
            elif wait:
                time.sleep(retry_interval)
            else:
                return None

        return task_res
