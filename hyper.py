
from copy import deepcopy
import itertools
from collections import OrderedDict

import numpy as np
from gridengine.task import Task


class HyperparamGrid:

    def __init__(self, ranges):

        #self.indexed_ranges = OrderedDict([(r[0], [(i, r[1][i]) for i in range(len(r[1]))]) for r in ranges])

        self.ranges = OrderedDict(ranges)
        self.range_indices = OrderedDict([(r[0], range(len(r[1]))) for r in ranges])

        self.axes = OrderedDict([(r[0], axis) for (r, axis) in zip(ranges, range(len(ranges)))])

        self.shape = [len(r[1]) for r in ranges]
        self.result_wrappers = OrderedDict()

    def save_result(self, measure, index_coord, value):
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