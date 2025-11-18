# Chapter 7: Interoperability and Data Federation

## For XTable:
Before you begin, ensure you have the following:
- Apache Spark Environment: A compute instance capable of running Spark. This can be your local machine, a Docker container, or a distributed service like Amazon EMR.
- Apache XTable Repository: Clone the Apache XTable repository and build the xtable-utilities_2.12-0.2.0-SNAPSHOT-bundled.jar by following the installation instructions provided in the official documentation.
- Optional: If you plan to read from or write to distributed storage services like Amazon S3 or Google Cloud Storage, ensure you have the necessary access configured.

Execute the following command from your terminal within the cloned Apache XTable directory to perform the metadata translation.
`java -jar xtable-utilities/target/xtable-utilities_2.12-0.2.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml`

## For Delta Uniform:
There are two different ways to use UniForm.
- Create a new Delta table with Uniform (refer to `delta_uniform.sql`)
- Enable UniForm on existing Delta Lake table (refer to `existing_delta_uniform.sql`)
