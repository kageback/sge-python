import pickle
import os
import sys
import time

sys.path.append(os.getcwd())

# pop arguments
arg_stack = sys.argv
task_path = arg_stack.pop()


# load task
with open(task_path, 'rb') as f:
    task = pickle.load(f)

args = task.args
kwargs = task.kwargs

print(task)

# call function
task.result = task.function(*args, **kwargs)

# save the results
with open(task_path, 'wb') as f:
    pickle.dump(task, f)
