# Jobs

Jobs is a library built on top of [rq]() and [Redis]() database that exposes RESTful API to manage job queues. It supports distributed batch processing of data defined as [datasets](https://github.com/vahana/datasets). 

## ETL

ETL is a built-in module that implements basic ETL operations over "data lake" uniformly over various backends/datasets.

It introduces a notion of `source`, `merger` and `target` [datasets](https://github.com/vahana/datasets)

Following are main operations over datasets:

1. read source dataset and create a target dataset

2. read source dataset, match with merger on a condition and create a target dataset (similar to sql joins)

3. read a source and update a target using supplied conditions and keys

4. read a source and delete from target

Note that ETL operations are agnostic to underlying database technology, so a dataset could have any of the supported backends. See [datasets](https://github.com/vahana/datasets) for list of currently supported backends. 



See the implementation of this library in [ETL](https://github.com/vahana/etl) project. 
