import pickle


class ResultWrapper:
    def __init__(self, path, sge_job_id):
        self.result_path = path
        self.sge_job_id = sge_job_id

    def set(self, result):
        # Todo Save numpy, pytorch and pandas using custom serialization

        # save the results
        with open(self.result_path + '.pkl', 'wb') as f:
            pickle.dump(result, f)

    def get(self):
        # Handle when not on disk (task run on another node)
        with open(self.result_path + '.pkl', 'rb') as f:
            return pickle.load(f)
