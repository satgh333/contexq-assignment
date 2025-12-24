# jm_requirement
#!/bin/bash

# EMR Bootstrap script to install dependencies
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install pyiceberg[s3fs,glue]==0.5.1
sudo python3 -m pip install mlflow==2.8.1
sudo python3 -m pip install python-Levenshtein==0.21.1
sudo python3 -m pip install boto3==1.29.7

# Download Iceberg Spark runtime
sudo wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.2/iceberg-spark-runtime-3.4_2.12-1.4.2.jar -P /usr/lib/spark/jars/
