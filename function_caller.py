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
result = task.function(*task.args, **task.kwargs)

# Print result to log
print('result =', result)

# save the results
with open(task.result_path, 'wb') as f:
    pickle.dump(result, f)
