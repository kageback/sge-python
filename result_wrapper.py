import pickle


class ResultWrapper:
    def __init__(self, parent_task, path):
        self.parent_task = parent_task

        self.result_path = path

    def set(self, result):
        # Todo Save numpy, pytorch and pandas using custom serialization

        # save the results
        with open(self.result_path + '.pkl', 'wb') as f:
            pickle.dump(result, f)

    def get(self):
        # Handle when not on disk (task run on another node)
        with open(self.result_path + '.pkl', 'rb') as f:
            return pickle.load(f)

    @property
    def sge_job_id(self):
        return self.parent_task.sge_job_id


class LocalResult(ResultWrapper):
    def __init__(self, result):
        self.result = result

    def get(self):
        return self.result

    def set(self, result):
        self.result = result

    @property
    def sge_job_id(self):
        raise NotImplemented("Local results have no SGE job id.")



