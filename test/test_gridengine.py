import unittest
from expgraph.rsync import SyncTo
from expgraph.gridengine.ge_queue import GEQueue
from expgraph.pipeline import Experiment, Local

import test.writing as writing


class TestGridEngine(unittest.TestCase):
    def test_sge_experiment(self):
        host = 'gridengine-frontend001.us-central1-a.sleeplabels'
        queue = GEQueue(cluster_wd='~/runtime/test/', host=host, user='kageback', queue_limit=4)
        queue.sync('.', '.', exclude=['pipelines/*'], sync_to=SyncTo.REMOTE,
                   recursive=True)

        exp = Experiment(exp_name='',
                         fixed_params=[('msg', 'Hello world')],
                         param_ranges=[('n', range(2))],
                         queue=queue)
        queue.sync(exp.pipeline_path, exp.pipeline_path, sync_to=SyncTo.REMOTE, recursive=True)

        exp_i = 0
        for (params_i, params_v) in exp:
            print('Scheduled %d experiments out of %d' % (exp_i, len(list(exp))))
            exp_i += 1
            task = exp.run(writing.writing, exp.fixed_params['msg'], params_v[exp.axes['n']])

            exp.set_result('msg', params_i, task.result(0))
            exp.set_result('n', params_i, task.result(1))

        exp.save()
        print("\nAll tasks queued to clusters")

        # wait for all tasks to complete
        exp.wait(retry_interval=5)
        exp.queue.sync(exp.pipeline_path, exp.pipeline_path, sync_to=SyncTo.LOCAL, recursive=True)

        print(exp.get_flattened_results('msg'))
        self.assertListEqual(exp.get_flattened_results('n'), list(exp.param_ranges['n']))


    def test_shorthand_iters(self):
        exp = Experiment(fixed_params=[('f1', 1),
                                       ('f2', '2')],
                         param_ranges=[('r1', [1, 2, 3]),
                                       ('r2', ['a', 'b', 'c'])])
        self.assertEqual(exp['f1'], exp.fixed_params['f1'])
        self.assertEqual(exp['f2'], exp.fixed_params['f2'])
        for ((i, v), (i_old, v_old)) in zip(exp.iter_ranges(), exp):
            self.assertEqual(i, i_old)
            self.assertEqual(v['r1'], v_old[exp.axes['r1']])
            self.assertEqual(v['r2'], v_old[exp.axes['r2']])

if __name__ == '__main__':
    unittest.main()
