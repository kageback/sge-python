import os
import sys
sys.path.append(os.getcwd())

import pickle as pickle
from gridengine.result_wrapper import ResultWrapper

def main(arg_stack):

    # pop arguments
    task_path = arg_stack.pop()

    # load task
    with open(task_path, 'rb') as f:
        task = pickle.load(f)

    # print func ref to log
    #print('function = ', task.function)

    # Print arguments to log
    #print('args =', task.args)
    #print('kwargs =', task.kwargs)

    # get wrapped results
    for i in range(len(task.args)):
        if isinstance(task.args[i], ResultWrapper):
            task.args[i] = task.args[i].get()
    for k in task.kwargs:
        if isinstance(task.kwargs[k], ResultWrapper):
            task.kwargs[k] = task.kwargs[k].get()

    # call function
    result = task.function(*task.args, **task.kwargs)

    # Add results
    if type(result) is tuple:
        for res, i in zip(result, range(len(result))):
            task.result(i).set(res)
    else:
        task.result(0).set(result)


if __name__ == '__main__':
    main(sys.argv)
