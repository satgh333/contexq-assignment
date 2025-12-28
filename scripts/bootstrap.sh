#!/bin/bash

# EMR Bootstrap script to install dependencies
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install mlflow==3.8.0
sudo python3 -m pip install python-Levenshtein==0.27.3
sudo python3 -m pip install boto3==1.35.0

# Iceberg Spark runtime is typically installed on EMR via configurations.
# If needed, you can install a compatible runtime jar for your EMR Spark version here.
