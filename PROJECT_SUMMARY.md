# jm_requirement
# Data & AI Engineering Pipeline - Project Structure

```
data-ai-pipeline/
├── README.md                           # Project documentation
├── requirements.txt                    # Python dependencies
├── setup.sh                           # Local setup script
├── src/
│   ├── etl_pipeline.py                # Main ETL/ML pipeline
│   ├── entity_resolution.py           # Deduplication logic
│   └── ml_training.py                 # ML model training
├── tests/
│   └── test_entity_resolution.py      # Unit tests
├── data/
│   ├── supply_chain_data.json         # Sample supply chain data
│   └── financial_data.json            # Sample financial data
├── .github/workflows/
│   └── ci-cd.yml                      # GitHub Actions pipeline
├── terraform/
│   └── main.tf                        # Infrastructure as Code
└── airflow/
    └── corporate_pipeline_dag.py      # Airflow orchestration
```

## Quick Start

1. **Clone and setup:**
   ```bash
   cd data-ai-pipeline
   ./setup.sh
   ```

2. **Deploy infrastructure:**
   ```bash
   cd terraform
   terraform init
   terraform apply
   ```

3. **Run pipeline:**
   ```bash
   python src/etl_pipeline.py
   ```

## Key Features Implemented

✅ **Entity Resolution**: Fuzzy matching with 85% similarity threshold  
✅ **Apache Iceberg**: ACID transactions with MERGE operations  
✅ **PySpark ML**: Logistic regression with feature engineering  
✅ **MLflow**: Model tracking and registry  
✅ **CI/CD**: Automated testing and deployment  
✅ **Infrastructure**: Terraform for AWS resources  
✅ **Orchestration**: Airflow DAG for scheduling  

The pipeline successfully demonstrates enterprise-grade data engineering with modern tools and best practices.
