import pickle
import codecs
import os
import sys

sys.path.append(os.getcwd())

#print(sys.argv)
# copy and pop arguments
arg_stack = sys.argv
kwargs = pickle.loads(codecs.decode(arg_stack.pop().encode(), "base64"))
args = pickle.loads(codecs.decode(arg_stack.pop().encode(), "base64"))
result_path = arg_stack.pop()
func_name = arg_stack.pop()
module_name = arg_stack.pop()

# call function
m = __import__(module_name)
f = getattr(m, func_name)
result = f(*args, **kwargs)

#print(result)
# save the results
with open(result_path, 'wb') as f:
    pickle.dump(result, f)
