import time
from gridengine.job import Job
import gridengine.task
from gridengine.misc import *
from gridengine import SyncTo
from gridengine.result_wrapper import ResultWrapper


class Grid:
    def __init__(self, jobs_base_path='jobs'):
        self.jobs_base_path = enforce_trailing_backslash(jobs_base_path)
        self.queues = []

    def add_queue(self, queue):
        self.queues.append(queue)

    def sync(self, local_path, cluster_path, sync_to, exclude=[]):
        for queue in self.queues:
            queue.sync(local_path, cluster_path, sync_to, exclude, recursive=True)

    def run_job(self, job, poll_interval=5):
        to_do = job.tasks
        while len(to_do) > 0:
            backlog = []
            for task in to_do:
                if self.dependencies_finished(task):
                    queue = self.get_free_queue()
                    task.schedule(queue)
                else:
                    backlog.append(task)

            to_do = backlog
            print('\r' + str(len(to_do)) + ' tasks left to schedule...', end="")
            time.sleep(poll_interval)
        self.wait(job, retry_interval=poll_interval)
        print("\n Done!")

    @staticmethod
    def dependencies_finished(task):
        for k in task.kwargs.keys():
            if isinstance(task.kwargs[k], ResultWrapper):
                if not task.kwargs[k].is_finished():
                    return False
        return True

    def run_function(self, f, *args, **kwargs):

        job = Job(jobs_base_path=self.jobs_base_path, job_id_prefix=slugify(f.__name__))
        queue = self.get_free_queue()
        task = job.run(queue, f, args, kwargs)
        self.wait(job)
        return task.get_result()

    def map(self, f, iterator):

        job = Job(jobs_base_path=self.jobs_base_path, job_id_prefix=slugify(f.__name__))
        tasks = []
        for args in iterator:
            args = ensure_iter(args)
            queue = self.get_free_queue()
            tasks.append(job.run(queue, f, args, {}))
        self.wait(job)
        #result = [task.get_result() for task in tasks]
        result = map(lambda task: task.get_result(), tasks)
        return result

    def get_free_queue(self, wait=True):
        free_env = None
        max_free_queue_slots = 0
        while wait and free_env is None:
            for env in self.queues:
                free_queue_slots = env.queue_slots_available()
                if free_queue_slots > max_free_queue_slots:
                    max_free_queue_slots = free_queue_slots
                    free_env = env
            if free_env is None:
                time.sleep(5)
        return free_env

    def wait(self, job, retry_interval=1):
        done = False
        while(not done):

            queued = 0
            running = 0
            error = 0
            for queue in self.queues:
                q, r, e = queue.queue_state(job)
                queued += q
                running += r
                error += e

            if queued + running > 0:
                print('\rWaiting for ' + str(running) + ' running and ' + str(queued) + ' queued tasks to finish', end="")
                time.sleep(retry_interval)
            elif error > 0:
                print('\nAll tasks terminated! ' + str(error) + ' tasks stuck in error state')
                done = True
            else:
                print('\nAll tasks completed!')
                done = True
