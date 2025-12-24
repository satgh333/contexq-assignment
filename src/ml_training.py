from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
import mlflow
import mlflow.spark

def engineer_features(df: DataFrame) -> DataFrame:
    df_features = df.withColumn("high_profit", when(col("profit") > 500000, 1).otherwise(0))
    df_features = df_features.fillna({
        "revenue": 0,
        "profit": 0,
        "supplier_count": 0
    })
    return df_features

def train_model(df: DataFrame) -> None:
    with mlflow.start_run():
        df_ml = engineer_features(df)
        assembler = VectorAssembler(inputCols=["revenue", "supplier_count"], outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="high_profit", maxIter=10)
        pipeline = Pipeline(stages=[assembler, lr])
        train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)
        evaluator = BinaryClassificationEvaluator(labelCol="high_profit")
        auc = evaluator.evaluate(predictions)
        mlflow.log_metric("auc", auc)
        mlflow.spark.log_model(model, "corporate_profit_model")
        print(f"Model trained with AUC: {auc}")
        return model
