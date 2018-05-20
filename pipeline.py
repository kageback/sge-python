import os
import time
import pickle as pickle
from functools import reduce

from gridengine.task import Task
from gridengine.result_wrapper import ResultWrapper
from gridengine.queue import Local

class Pipeline:
    def __init__(self, queue=None, pipelines_path='pipelines', pipeline_id_prefix='pl'):
        if queue is None:
            queue = Local()

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


##### HyperParamSearchExperiment ######

from copy import deepcopy
import itertools
from collections import OrderedDict

import numpy as np
class HyperParamSearchExperiment(Pipeline):

    def __init__(self, ranges, exp_name='exp', queue=None, pipelines_path='pipelines'):
        super().__init__(queue, pipelines_path, exp_name)

        self.ranges = OrderedDict(ranges)
        self.range_indices = OrderedDict([(r[0], range(len(r[1]))) for r in ranges])

        self.axes = OrderedDict([(r[0], axis) for (r, axis) in zip(ranges, range(len(ranges)))])

        self.shape = [len(r[1]) for r in ranges]
        self.result_wrappers = OrderedDict()

    def set_result(self, measure, index_coord, value):
        def add(list_obj, coord, value):
            if len(coord) > 1:
                add(list_obj[coord[0]], coord[1:], value)
            else:
                list_obj[coord[0]] = value

        if measure not in self.result_wrappers.keys():
            self.result_wrappers[measure] = np.zeros(self.shape).tolist()

        add(self.result_wrappers[measure], index_coord, value)


    def to_numpy(self, output_name, result_index=0):
        def unwrap_results(list_obj):

            for i in range(len(list_obj)):
                if type(list_obj[i]) is Task:
                    list_obj[i] = list_obj[i].result(result_index).get()
                else:
                    unwrap_results(list_obj[i])

        results = deepcopy(self.result_wrappers[output_name])
        unwrap_results(results)

        return np.array(results)

    def __iter__(self):
        #return itertools.product(*self.indexed_ranges.values())
        return zip(itertools.product(*self.range_indices.values()), itertools.product(*self.ranges.values()))