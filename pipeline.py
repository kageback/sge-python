import os
import time
import pickle as pickle
from functools import reduce

from gridengine.task import Task
from gridengine.result_wrapper import ResultWrapper


class Pipeline:
    def __init__(self, queue, pipelines_path='pipelines', pipeline_id_prefix='pl'):
        self.queue = queue

        pipelines_path = os.path.dirname(pipelines_path + '/') + '/'

        self.tasks = []

        # Create pipeline output directory
        if not os.path.isdir(pipelines_path):
            print('Pipelines directory missing. Creating directory: ' + os.path.realpath(pipelines_path))
            os.mkdir(pipelines_path)
        i = 0
        while(True):
            self.pipeline_name = pipeline_id_prefix + '.' + str(i)
            self.pipeline_path = pipelines_path + self.pipeline_name + '/'

            if os.path.isdir(self.pipeline_path):
                i += 1
            else:
                os.mkdir(self.pipeline_path)
                break

    def run(self, f, *args, **kwargs):
        dependencies = self.get_job_dependencies(args, kwargs)

        task_name = self.pipeline_name + '.' + str(len(self.tasks))

        task = Task(f, args, kwargs, task_name, self.pipeline_path, dependencies)
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
        with open(self.pipeline_path + "pipeline.pkl", 'wb') as f:
            pickle.dump(self, f)

    @staticmethod
    def load(pipeline_id, pipelines_path='pipelines'):
        pipelines_path = os.path.dirname(pipelines_path + '/') + '/'
        save_path = pipelines_path + pipeline_id + "/pipeline.pkl"
        with open(save_path, 'rb') as f:
            return pickle.load(f)


