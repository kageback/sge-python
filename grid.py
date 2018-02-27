import time
from gridengine.job import Job
from gridengine.misc import *


class Grid:
    def __init__(self, local_wd='./', jobs_base_path='jobs', exclude=''):
        self.local_wd = local_wd
        self.jobs_base_path = enforce_trailing_backslash(jobs_base_path)
        self.exclude = exclude
        self.queues = []

    def add_queue(self, queue):
        queue.sync_to_cluster(self.local_wd, queue.cluster_wd, self.exclude)
        self.queues.append(queue)

    def run_function(self, f, *args, **kwargs):
        comp_env = self.get_free_queue()
        job = Job(jobs_base_path=self.jobs_base_path, job_id_prefix=slugify(f.__name__))
        task = job.run_function(comp_env, f, args, kwargs)
        self.wait(job)
        return task.get_result()

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

    def wait(self, job):
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
                time.sleep(10)
            elif error > 0:
                print('\nAll tasks terminated! ' + str(error) + ' tasks stuck in error state')
                done = True
            else:
                print('\nAll tasks completed!')
                done = True
