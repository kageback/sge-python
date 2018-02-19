import pickle
import codecs
import os
import sys
import time

sys.path.append(os.getcwd())

# copy and pop arguments
arg_stack = sys.argv
result_path = arg_stack.pop()
args_path = arg_stack.pop()
func_name = arg_stack.pop()
module_name = arg_stack.pop()


# load arguments
arguments = None
while arguments is None:
    try:
        with open(args_path, 'rb') as f:
            arguments = pickle.load(f)
    except FileNotFoundError:
        time.sleep(1)
args = arguments[0]
kwargs = arguments[1]

# call function
m = __import__(module_name)
f = getattr(m, func_name)
result = f(*args, **kwargs)

#print(result)
# save the results
with open(result_path, 'wb') as f:
    pickle.dump(result, f)
