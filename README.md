# sge-python

If you are running python experiments on a GridEngine (GE) cluster (or potentially your own computer with a local GE queue) this lib might be of use for you. It was developed by me during my PHD in Machine Learning so the use cases it has been tested for are mainly connected to that area. That said, the lib was designed to be agnostic to the task.  

## Design philosophy
To understand how to use the lib it is useful to understand how I was thinking designing it (I think).
My main goal was to make the underling computational envioment transparent to the user and to achive this I use the generic function as interface. That is, you schould ideally be able to simply run any function via the library and get a result just as if you had run it locally. This makes it easy run code that was not specifically designed to run on a grid and also easy to debug your code locally by simply choosing to execute locally, i.e. without changing the code (except to flag that it is to be run locally).

## Use cases

### one of Job

### Pipeline
If you need to run a pipeline (or network) of Jobs depending on the output of other jobs if is as easy as creating a Pipeline object and running the Jobs through that. The dependencies are defined by using the output result wrapper as an argument for the next Job.

### GPU allocation
This lib has support for allocating GPU resources on the grid. However, it assumes that the SGE instance has been set up using using to handle GPUs as a resource. E.g. using https://github.com/kageback/sge-gpuprolog

## installation
