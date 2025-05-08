import math
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.mllib.evaluation import MulticlassMetrics


### Spark running and connecting to DB

# Spark session start
team = "team15"
warehouse = "project/hive/warehouse"
spark = SparkSession.builder\
        .appName(f"{team} - spark ML")\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .enableHiveSupport()\
        .getOrCreate()
print("Spark Session created.")

# Data loading from Hive
hive_db_name = f"{team}_projectdb"
hive_table_name = "flight"
df = spark.read \
    .format("hive") \
    .option("database", hive_db_name) \
    .table(hive_table_name)


### Data preparation

# Selecting features (drop unnecessary columns)
features = [
    'year', 'month', 'dayofmonth', 'dayofweek',
    'crsdeptime', 'crsarrtime',
    'crselapsedtime',
    'origin', 'dest', 'distance',
]
label = "cancelled"
df = df.select(features + [label])
df = df.withColumn(label, F.col(label).cast(DoubleType()))

# Handle missing values
df = df.na.drop()

# Time features decomposition
df = df \
    .withColumn("month_sin", F.sin(2*math.pi * F.col("month") / 12.0))\
    .withColumn("month_cos", F.cos(2*math.pi * F.col("month") / 12.0))\
    .withColumn("dayofmonth_sin", F.sin(2*math.pi * F.col("dayofmonth") / 31.0))\
    .withColumn("dayofmonth_cos", F.cos(2*math.pi * F.col("dayofmonth") / 31.0))\
    .withColumn("dayofweek_sin", F.sin(2*math.pi * F.col("dayofweek") / 7.0))\
    .withColumn("dayofweek_cos", F.cos(2*math.pi * F.col("dayofweek") / 7.0))

def split_hhmm(colname):
    return (
        F.floor(F.col(colname) / 100).cast(IntegerType()),
        (F.col(colname) % 100).cast(IntegerType())
    )

time_features = ['crsdeptime', 'crsarrtime']
for tf in time_features:
    hr, mn = split_hhmm(tf)
    df = df \
        .withColumn(f"{tf}_hr_sin", F.sin(2*math.pi * hr / 24.0)) \
        .withColumn(f"{tf}_hr_cos", F.cos(2*math.pi * hr / 24.0)) \
        .withColumn(f"{tf}_mn_sin", F.sin(2*math.pi * mn / 60.0)) \
        .withColumn(f"{tf}_mn_cos", F.cos(2*math.pi * mn / 60.0))

# Pipeline:
# 1. One-hot encoding for categorical features
# 2. VectorAssembler for numeric features
# 3. StandardScaler
cats = ["origin", "dest", "year"]
num_feats = ["crselapsedtime", "distance"]
cyc_feats = (
    ["month_sin", "month_cos", "dayofmonth_sin", "dayofmonth_cos", "dayofweek_sin", "dayofweek_cos"] +
    [f"{tf}_{p}_{axis}"
     for tf in time_features
     for p in ["hr", "mn"]
     for axis in ["sin", "cos"]]
)
all_numeric = num_feats + cyc_feats

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in cats
]
ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in cats], outputCols=[f"{c}_ohe" for c in cats], dropLast=True)
assembler = VectorAssembler(inputCols=all_numeric + [f"{c}_ohe" for c in cats], outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
pipeline = Pipeline(stages=indexers + [ohe, assembler, scaler])
model = pipeline.fit(df)
df_prepared = model.transform(df).select("features", "cancelled")

# Balance dataset by down-sampling the majority class
counts = df_prepared.groupBy("cancelled").count().collect()
cnts = {row["cancelled"]: row["count"] for row in counts}
min_class = min(cnts, key=cnts.get)
maj_class = max(cnts, key=cnts.get)
fraction = float(cnts[min_class]) / float(cnts[maj_class])
fractions = {min_class: 1.0, maj_class: fraction}
df_balanced = df_prepared.sampleBy("cancelled", fractions, seed=42)

# Split into train/val sets
df_class_0 = df_balanced.filter(F.col("cancelled") == 0.0)
df_class_1 = df_balanced.filter(F.col("cancelled") == 1.0)
train_0, val_0 = df_class_0.randomSplit([0.8, 0.2], seed=42)
train_1, val_1 = df_class_1.randomSplit([0.8, 0.2], seed=42)
train_df = train_0.unionAll(train_1)
val_df = val_0.unionAll(val_1)

def run(command):
    return os.popen(command).read()

# Save data to HDFS
train_df.select("features", "cancelled")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/train")
run("hdfs dfs -cat project/data/train/*.json > data/train.json")

val_df.select("features", "cancelled")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/val")
run("hdfs dfs -cat project/data/val/*.json > data/val.json")


### ML modelling

label_col = "cancelled"
features_col = "features"

# Define ML model 1 - Logistic Regression
lr = LogisticRegression(labelCol=label_col, featuresCol=features_col)
paramGrid_lr = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .addGrid(lr.maxIter, [10, 50]) \
    .build()
evaluator1 = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction", metricName="areaUnderROC")
cv_lr = CrossValidator(estimator=lr,
                       estimatorParamMaps=paramGrid_lr,
                       evaluator=evaluator1,
                       numFolds=3,
                       parallelism=5,
                       seed=42)

# Function to canculate classification metrics
def get_detailed_metrics(predictions_df, model_name="Model"):
    print(f"\nDetailed Metrics for {model_name} on Test Data:")

    preds_and_labels = predictions_df.select(
        F.col("prediction").cast(DoubleType()),
        F.col(label_col).cast(DoubleType())
    ).rdd.map(lambda r: (r[0], r[1]))

    metrics = MulticlassMetrics(preds_and_labels)
    print(f"  Confusion Matrix:\n{metrics.confusionMatrix().toArray()}")
    print(f"  Precision (Cancelled=1.0): {metrics.precision(1.0):.4f}")
    print(f"  Recall (Cancelled=1.0): {metrics.recall(1.0):.4f}")
    print(f"  F1-Score (Cancelled=1.0): {metrics.fMeasure(1.0):.4f}")
    print(f"  Accuracy: {metrics.accuracy:.4f}")
    return metrics.accuracy, metrics.precision(1.0), metrics.recall(1.0), metrics.fMeasure(1.0)

# Train model
cv_model_lr = cv_lr.fit(train_df)
best_lr_model = cv_model_lr.bestModel

# Evaluate model
predictions_lr = best_lr_model.transform(val_df)
auc_lr_test = evaluator1.evaluate(predictions_lr)
lr_accuracy, lr_precision, lr_recall, lr_f1 = get_detailed_metrics(predictions_lr, "Logistic Regression")

# Save model
model_output_path_lr = f"project/models/logistic_regression_model"
best_lr_model.write().overwrite().save(model_output_path_lr)
run("hdfs dfs -get project/models/logistic_regression_model models/logistic_regression_model")

# Save predictions
predictions_output_path_lr = f"project/output/lr_model_predictions.csv"
predictions_lr.select(label_col, "prediction") \
    .coalesce(1) \
    .write.mode("overwrite").format("csv") \
    .option("sep", ",")\
    .option("header", "true") \
    .save(predictions_output_path_lr)
run("hdfs dfs -cat project/output/lr_model_predictions.csv/*.csv > output/lr_model_predictions.csv")


# Define ML model 2 - Random Forest
rf = RandomForestClassifier(labelCol=label_col, featuresCol=features_col, seed=42)
paramGrid_rf = ParamGridBuilder() \
    .addGrid(rf.numTrees, [20, 50]) \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .addGrid(rf.featureSubsetStrategy, ["sqrt", "log2"]) \
    .build()
evaluator2 = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction", metricName="areaUnderROC")
cv_rf = CrossValidator(estimator=rf,
                       estimatorParamMaps=paramGrid_rf,
                       evaluator=evaluator2,
                       numFolds=3,
                       parallelism=5,
                       seed=42)

# Train model
cv_model_rf = cv_rf.fit(train_df)
best_rf_model = cv_model_rf.bestModel

# Evaluate model
predictions_rf = best_rf_model.transform(val_df)
auc_rf_test = evaluator2.evaluate(predictions_rf)
rf_accuracy, rf_precision, rf_recall, rf_f1 = get_detailed_metrics(predictions_rf, "Random Forest")

# Save model
model_output_path_rf = f"project/models/random_forest_model"
best_rf_model.write().overwrite().save(model_output_path_rf)
run("hdfs dfs -get project/models/random_forest_model models/random_forest_model")

# Save predictions
predictions_output_path_rf = f"project/output/rf_model_predictions.csv"
predictions_rf.select(label_col, "prediction") \
    .coalesce(1) \
    .write.mode("overwrite").format("csv") \
    .option("sep", ",")\
    .option("header", "true") \
    .save(predictions_output_path_rf)
run("hdfs dfs -cat project/output/rf_model_predictions.csv/*.csv > output/rf_model_predictions.csv")


### Compare models

# Create evaluation table
models = [
    [str(best_lr_model), auc_lr_test, lr_accuracy, lr_precision, lr_recall, lr_f1],
    [str(best_rf_model), auc_rf_test, rf_accuracy, rf_precision, rf_recall, rf_f1]
]
df = spark.createDataFrame(models, ["Model", "AUC", "Accuracy", "Precision", "Recall", "F1-score"])

# Save evaluation table
df.coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .save("project/output/evaluation.csv")
run("hdfs dfs -cat project/output/evaluation.csv/*.csv > output/evaluation.csv")

# Stop Spark session
spark.stop()
