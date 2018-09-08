# sge-python

SGE-Python makes it easy to run python code on a GridEngine (GE) cluster and help you manage the results, getting rid of the need to create shell script or manually submitting jobs to the queue . 

SGE-Python is designed to make the underlying execution transperent so that you can run the same code when developing/debugging localally as when running the your experiment on the cluster. Further, it lets you shedule the entire experiment at ones even when there are dependencies between the jobs and GE will subseqently run each job in parallel by stepping through the job dependency graph. This makes hyperparameter search in partally shared pipelines, e.g. where some of the initial steps in the pipeline is shared between experiments, easy to set up and efficent whichout loosing traceablitity of each processing step taken for each individual hyperparameter setting.   

Small disclaimer: SGE-Python was developed and used by me during my PhD in Machine Learning so the use cases it has been tested for are mainly connected to that area.  

## Example Use cases

### One of Job

### Pipeline
If you need to run a pipeline (or network) of Jobs depending on the output of other jobs if is as easy as creating a Pipeline object and running the Jobs through that. The dependencies are defined by using the output result wrapper as an argument for the next Job.

### Hyper Param Search

## installation
All you need to do to use the lib is to Clone/download the repositry to some place you can import from. However, the recomended best practice is to add it as a submodule in the root folder of your project by running: git submodule add https://github.com/kageback/sge-python.git sge-python


## GPU allocation
This lib has support for allocating GPU resources on the grid. However, it assumes that the SGE instance has been set up using using to handle GPUs as a resource. E.g. using https://github.com/kageback/sge-gpuprolog

