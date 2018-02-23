import os
import pickle

class Task:
    def __init__(self, comp_env, task_name_prefix="task", task_number=0):
        self.task_number = task_number
        self.comp_env = comp_env
        self.task_name_prefix = task_name_prefix


    def run_function(self, f, args, kwargs):

        module_name = f.__module__
        func_name = f.__name__

        self_path = os.path.dirname(os.path.realpath(__file__))
        self_relative_project_path = os.path.relpath(self_path, '.')

        with open(self.get_args_path(), 'wb') as f:
            pickle.dump([args, kwargs], f)

        self.comp_env.sync_to_cluster(self.comp_env.local_wd   + self.comp_env.output_folder,
                                      self.comp_env.cluster_wd + self.comp_env.output_folder)

        self.comp_env.submit_job(self.get_task_name(),
                                                  [self_relative_project_path + '/function_caller.py',
                                                  module_name,
                                                  func_name,
                                                  self.get_args_path(),
                                                  self.get_result_path()])

    def get_result(self, wait=True):
        result_path = self.get_result_path()
        task_res = None
        while wait and task_res is None:
            if not os.path.isfile(result_path):
                self.task_env[task_id].sync_results()

            try:
                with open(result_path, 'rb') as f:
                    task_res = pickle.load(f)
            except FileNotFoundError:
                time.sleep(60)

        return task_res


    def get_result_path(self):
        return self.comp_env.output_folder + self.get_task_name() + '.func_res.pkl'

    def get_args_path(self):
        return self.comp_env.output_folder + self.get_task_name() + '.func_args.pkl'

    def get_task_name(self):
        return self.task_name_prefix + '.' + str(self.task_number)
