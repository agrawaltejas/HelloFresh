# HelloFresh PySpark Project

This document is designed to use best practices of Spark and Python to process the csv file from input , transform it, and load it to final location.
Following practices have been used on a high-level : 

- how to structure ETL code in such a way that it can be easily tested and debugged;
- how to pass configuration parameters to a PySpark job;
- how to handle dependencies on other modules and packages;
- what constitutes a 'meaningful' test for an ETL job;
- how to implement CI/CD for the process using JenkinsFile;
- how to run pipeline in local as well as cluster level.

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- config/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- logging.py
 |   |-- spark.py
 |-- src/
 |   |-- main
         |-- extractor
             |-- extract.py
         |-- transformer
             |-- transform.py
         |-- loader
             |-- load.py
         |-- etl_job.py
     |-- test 
         |-- test_data
         |-- etl_tests.py
 |   input
 |   output
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
 |   Jenkinsfile
```

The main Python module containing the ETL job (which will be sent to the Spark cluster), is `src/main/etl_job.py`. Any external configuration parameters required by `etl_job.py` are stored in JSON format in `config/etl_config.json`. Additional modules that support this job can be kept in the `dependencies` folder . In the project's root we include `build_dependencies.sh`, which is a bash script for building these dependencies into a zip-file to be sent to the cluster (`packages.zip`). Unit test modules are kept in the `src/main/tests` folder and small chunks of representative input and output data, to be used with the tests, are kept in `src/main/test/test_data` folder. Min unit test file is `src/main/test/etl_tests.py`

## Structure of an ETL Job

In order to facilitate easy debugging and testing, we recommend that the 'Transformation' step be isolated from the 'Extract' and 'Load' steps, into its own function - taking input data arguments in the form of DataFrames and returning the transformed data as a single DataFrame. Then, the code that surrounds the use of the transformation function in the `main()` job function, is concerned with Extracting the data, passing it to the transformation function and then Loading (or writing) the results to their ultimate destination. Testing is simplified, as mock or test data can be passed to the transformation function and the results explicitly verified, which would not be possible if all of the ETL code resided in `main()` and referenced production data sources and destinations.


## Packaging ETL Job Dependencies

In this project, functions that can be used across different ETL jobs are kept in a module called `dependencies` and referenced in specific job modules using, for example,

```python
from dependencies.spark import start_spark
```

This package, together with any additional dependencies referenced within it, must be copied to each Spark node for all jobs that use `dependencies` to run :

Send all dependencies as a `zip` archive together with the job, using `--py-files` with Spark submit;
we have provided the `build_dependencies.sh` bash script for automating the production of `packages.zip`, given a list of dependencies documented in `Pipfile` and managed by the `pipenv` python application.

## Running the ETL job [Local Mode - Pipenv]

- We need to have first installed Python globally in local. I am using Python 3.10 version.;
- As I am using `Pipenv`, we need to install it and load the dependencies from dev packages of `Pipfile` or `Pipfile.lock`;
- Install Pipenv 
    ```bash 
   pip install pipenv
   pip install --upgrade pipenv
    ``` 
- If terminal fails to identify Pipenv , we may need to add Scripts path to PATH variable :
    ```bash 
   python -m site --user-site
   #example result : C:\Users\tejas\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.10_qbz5n2kfra8p0\LocalCache\local-packages\Python310\site-packages
   #Add this Path to PATH variable , with `site-packages` replaced with `Scripts` : 
   setx PATH "%PATH%;C:\Users\tejas\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.10_qbz5n2kfra8p0\LocalCache\local-packages\Python310\Scripts"
    ```  
- You should be in the root directory. First install all dependencies using Pipenv. Dev packages are given in Pipfile in root directory.
    ```bash 
   pip install --dev 
   # This will install development dependencies lile py4j , pyspark etc
    ```  
- Now , we are good to go with running our Pyspark application using Pipenv : 
     ```bash 
    pipenv run python3 .\src\main\etl_job.py
    ```  
- We will use following command to run the unitest cases :
     ```bash 
     pipenv run python -m unittest .\src\test\etl_tests.py
    ```  

## Running the ETL job [Cluster Mode]

Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder, then the ETL job can be run from the project's root directory using the following command from the terminal,

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--packages 'com.somesparkjar.dependency:1.0.0' \
--py-files packages.zip \
--files config/etl_config.json \
src/main/etl_job.py
```

Briefly, the options supplied serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--packages 'com.somesparkjar.dependency:1.0.0,...'` - Maven coordinates for any JAR dependencies required by the job (e.g. JDBC driver for connecting to a relational database);
- `--files configs/etl_config.json` - the (optional) path to any config file that may be required by the ETL job;
- `--py-files packages.zip` - archive containing Python dependencies (modules) referenced by the job; and,
- `jobs/etl_job.py` - the Python module file containing the ETL job to execute.


## Automated Testing

In order to test with Spark, we use the `pyspark` Python package, which is bundled with the Spark JARs required to programmatically start-up and tear-down a local Spark instance, on a per-test-suite basis (we recommend using the `setUpClass` and `tearDown` methods in `unittest.TestCase` to do this once per test-suite). Note, that using `pyspark` to run Spark is an alternative way of developing with Spark as opposed to using the PySpark shell or `spark-submit`.

Given that we have chosen to structure our ETL jobs in such a way as to isolate the 'Transformation' step into its own function (see 'Structure of an ETL job' above), we are free to feed it a small slice of 'real-world' production data that has been persisted locally - e.g. in `tests/test_data` or some easily accessible network directory - and check it against known results (e.g. computed manually or interactively within a Python interactive console session).

To execute the example unit test for this project run,

```bash
pipenv run python -m unittest .\src\test\etl_tests.py
```

### Automatic Loading of Environment Variables

Pipenv will automatically pick-up and load any environment variables declared in the `.env` file, located in the package's root directory. For example, adding,

```bash
SPARK_HOME=applications/spark-2.3.1/bin
HADOOP_HOME=application/hadoop-2.7.1/bin
DEBUG=1
```

## CI/CD Process
`Jenkinsfile` — It defines the CICD process. where the Jenkins agent runs the docker container defined in the Dockerfile in the prepare step followed by running the test. Once the test is successful in the prepare artifact step, it uses the makefile to create a zipped artifact. The final step is to publish the artifact which is the deployment step.

We need to have a Jenkins setup where we define a pipeline project and point to the Jenkins file

## Performance Tuning

We can use many practices to optimize the code pipeline and increase the runtime if data gets huge:

- Use of Parquet file format for storing final files, as it's best suitable for storing row columnar data;
- Use of colaesce() to process the data with partitions and without shuffling. I have used it while writing data to csv;
- Use of correct executors,cores and memory in spark can do wonders when we need to process high amount of data. With strong knowledge of architecture and trial method, we can find the best settings as per our requirements;
- We should persist the data if it is going to be used multiple times.
- We can avoid shuffling operations such as groupByKey(),reduceByKey() , and should replace joins with window functions wherever possible;
- We should use less actions in the pipeline such as count() or show() as it computes the whole DAG again and again at every action , which can reduce running time & lazy evaluation will not take effect to it's fullest.
- We should test our pipelines with peak testing data , to see if our pipelines are handling the huge data well.

## Production Deployment 

We may use production environment platforms such as oozie, airflow etc to deploy our pipelines. In our case, we can create a JAR with dependencies , config files and main entry class, and should use this JAR as an item in the workflow. We should have checks in place to see the workflow goes ahead only if this item runs successfully.

We can trigger the workflow intraday/daily/weekly based on our requirement. 

