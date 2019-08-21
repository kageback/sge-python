import os
import sys
sys.path.append(os.getcwd())

import pickle as pickle


def main(arg_stack):

    # pop arguments
    task_path = arg_stack.pop()

    # load task
    with open(task_path, 'rb') as f:
        task = pickle.load(f)

    task.execute()

if __name__ == '__main__':
    main(sys.argv)
