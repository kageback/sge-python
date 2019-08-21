import expgraph.function_caller as function_caller


class Queue:
    def sync(self, local_path, cluster_path, sync_to, exclude=[], recursive=False):
        pass

    def submit_job(self, task_name, args, log_folder="./", dependencies=[]):
        pass

    def queue_stat(self, sge_job_ids=[]):
        pass

    def is_job_finished(self, sge_job_id):
        pass


class Local(Queue):
    def submit_job(self, task_name, args, log_folder="./", dependencies=[]):
        function_caller.main(args[1:])
        return 1

    def queue_stat(self, sge_job_ids=None):
        return {}

    def is_job_finished(self, sge_job_id):
        return True

    def queue_slots_available(self):
        return 1
