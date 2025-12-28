#!/bin/bash

# Setup script for local development

echo "Setting up Data & AI Engineering Pipeline..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

echo "Setup complete!"
echo ""
echo "Next:"
echo "  - Run 'pytest -q' to run unit tests"
echo "  - Run 'python -m src.etl_pipeline' to execute the pipeline locally"
