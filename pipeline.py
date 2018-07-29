import os
import time
import pickle as pickle
from functools import reduce

from scipy.stats import t

from gridengine.task import Task
from gridengine.result_wrapper import ResultWrapper, LocalResult
from gridengine.ge_queue import Local

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
                self.task_path = self.pipeline_path + 'tasks/'
                os.mkdir(self.task_path)
                break

    def run(self, f, *args, **kwargs):
        dependencies = self.get_job_dependencies(args, kwargs)

        task_name = self.pipeline_name + '.' + str(len(self.tasks))

        task = Task(f, args, kwargs, task_name, self.task_path, dependencies)
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


##### Experiment ######

from copy import deepcopy
import itertools
from collections import OrderedDict

import numpy as np
class Experiment(Pipeline):

    def __init__(self, fixed_params=[], param_ranges=[], exp_name='exp', queue=None, pipelines_path='pipelines'):
        super().__init__(queue, pipelines_path, exp_name)

        self.fixed_params = OrderedDict(fixed_params)

        self.param_ranges = OrderedDict(param_ranges)
        self.range_indices = OrderedDict([(r[0], range(len(r[1]))) for r in param_ranges])

        self.axes = OrderedDict([(r[0], axis) for (r, axis) in zip(param_ranges, range(len(param_ranges)))])

        self.shape = [len(r[1]) for r in param_ranges]
        self.result_wrappers = OrderedDict()

    # Store the task representing the result of a particular parameter setting
    def set_result(self, measure_name, index_coord, value):
        def add(list_obj, coord, value):
            if len(coord) > 1:
                add(list_obj[coord[0]], coord[1:], value)
            else:
                list_obj[coord[0]] = value

        if not isinstance(value, ResultWrapper):
            value = LocalResult(value)

        if measure_name not in self.result_wrappers.keys():
            self.result_wrappers[measure_name] = np.zeros(self.shape).tolist()

        add(self.result_wrappers[measure_name], index_coord, value)

    # Retrieve result from stored tasks corresponding to a measure and put into a numpy array.
    def to_numpy(self, measure_name):
        def unwrap_results(list_obj):
            # Recursively retrieve all results
            for i in range(len(list_obj)):
                if isinstance(list_obj[i], ResultWrapper):
                    list_obj[i] = list_obj[i].get()
                else:
                    unwrap_results(list_obj[i])

        results = deepcopy(self.result_wrappers[measure_name])
        unwrap_results(results)

        return np.array(results)

    # estimate mean and corresponding confidence interval
    def estimate_mean(self, measure_name, as_function_of_axes=[], confidence_interval=0.95):
        x = self.to_numpy(measure_name)
        keep_axes = [self.axes[axis] for axis in as_function_of_axes]
        x = np.moveaxis(x, keep_axes, range(len(keep_axes)))

        # Collapse dimensions to average
        x = x.reshape(list(x.shape[0:len(keep_axes)]) + [-1])
        n = x.shape[-1]

        mean = x.mean(axis=-1)
        stddev = x.std(axis=-1, ddof=1)  # Sample variance

        # Get the endpoints of the range that contains 95% of the distribution
        # The degree used in calculations is N - ddof
        t_bounds = t.interval(confidence_interval, n - 1)
        ci = [mean + c * stddev / np.sqrt(n) for c in t_bounds]

        return mean, ci

    def get_reduced(self, measure_name, keep_axes_named=[], reduce_method='avg'):
        x = self.to_numpy(measure_name)
        keep_axes = [self.axes[axis] for axis in keep_axes_named]
        x = np.moveaxis(x, keep_axes, range(len(keep_axes)))
        if reduce_method == 'avg':
            res = x.mean(axis=tuple(range(len(keep_axes), len(self.shape))))
        elif reduce_method == 'std':
            res = x.std(axis=tuple(range(len(keep_axes), len(self.shape))))
        else:
            raise ValueError('unsupported reduce function: ' + reduce_method)
        return res

    # Function that returns all results corresponing to a measure as one dim list
    def get_flattened_results(self, measure_name):
        def flatten_results(list_obj, flattened_results):
            # Recursively retrieve all results
            for i in range(len(list_obj)):
                if isinstance(list_obj[i], ResultWrapper):
                    flattened_results += [list_obj[i].get()]
                else:
                    flatten_results(list_obj[i], flattened_results)

        flattened_results = []
        flatten_results(self.result_wrappers[measure_name], flattened_results)
        return flattened_results

    def __iter__(self):
        #return itertools.product(*self.indexed_ranges.values())
        return zip(itertools.product(*self.range_indices.values()), itertools.product(*self.param_ranges.values()))