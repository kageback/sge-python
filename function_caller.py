import dill as pickle
import os
import sys


def main(arg_stack):

    # pop arguments
    task_path = arg_stack.pop()

    # load task
    with open(task_path, 'rb') as f:
        task = pickle.load(f)

    # print func ref to log
    print('function = ', task.function)

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

if __name__ == '__main__':
    sys.path.append(os.getcwd())
    main(sys.argv)
