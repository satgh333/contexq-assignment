# jm_requirement
#!/bin/bash

# Setup script for local development

echo "Setting up Data & AI Engineering Pipeline..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export AWS_REGION=us-east-1
export GLUE_DATABASE=corporate_data
export S3_BUCKET=your-data-bucket-name

# Initialize MLflow tracking
mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlruns --host 0.0.0.0 --port 5000 &

echo "Setup complete!"
echo "MLflow UI available at: http://localhost:5000"
echo "Run 'python src/etl_pipeline.py' to execute the pipeline"
