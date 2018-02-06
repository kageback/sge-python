import pickle
import codecs
import sys


# copy and pop arguments
arg_stack = sys.argv
kwargs = pickle.loads(codecs.decode(arg_stack.pop(), "base64"))
args = pickle.loads(codecs.decode(arg_stack.pop(), "base64"))
result_path = arg_stack.pop()
func_name = arg_stack.pop()
module_name = arg_stack.pop()


m = __import__(module_name)
f = getattr(m, func_name)
result = f(*args, **kwargs)

with open(result_path, 'wb') as f:
    pickle.dump(result, f)

#print(res)

#pickled_res = codecs.encode(pickle.dumps(res), "base64").decode()

#sys.stdout.write(pickled_res)
