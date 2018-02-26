import pickle
import os
import sys

sys.path.append(os.getcwd())

# pop arguments
arg_stack = sys.argv
task_path = arg_stack.pop()


# load task
with open(task_path, 'rb') as f:
    task = pickle.load(f)

# Print arguments to log
print('args =', task.args)
print('kwargs =', task.kwargs)

# call function
task.result = task.function(*task.args, **task.kwargs)

# save the results
with open(task_path, 'wb') as f:
    pickle.dump(task, f)
