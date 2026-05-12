from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, Imputer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.storagelevel import StorageLevel
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import Binarizer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StandardScaler


# ------------------------------
# 1. Spark Session
# ------------------------------
team = "team26"
warehouse = f"project/hive/warehouse_{team}"

spark = SparkSession.builder \
    .appName("{} - spark ML".format(team)) \
    .master("yarn") \
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
    .config("spark.sql.warehouse.dir", warehouse) \
    .config("spark.sql.avro.compression.codec", "snappy") \
    .enableHiveSupport() \
    .getOrCreate()


print("spark.master =", spark.sparkContext.master)
print("deployMode =", spark.conf.get("spark.submit.deployMode", "(none)"))
print("applicationId =", spark.sparkContext.applicationId)


# ------------------------------
# 2. Load data
# ------------------------------
df = spark.table("team26_projectdb.chess_moves")
print("Initial row count:", df.count())

# ------------------------------
# 3. Data preprocessing (imputation, feature engineering)
# ------------------------------
# filling avg_time_spent_per_move_so_far
df = df.withColumn(
    "avg_time_spent_missing",
    F.col("avg_time_spent_per_move_so_far").isNull().cast("int")
)

med = (df
       .where(F.col("avg_time_spent_per_move_so_far").isNotNull())
       .groupBy("time_class")
       .agg(F.expr("percentile_approx(avg_time_spent_per_move_so_far, 0.5)").alias("med_avg_time"))
      )

df = (df.join(med, on="time_class", how="left")
        .withColumn(
            "avg_time_spent_per_move_so_far",
            F.coalesce(F.col("avg_time_spent_per_move_so_far"), F.col("med_avg_time"))
        )
        .drop("med_avg_time")
     )


tc = F.trim(F.col("time_control_raw"))

daily_slash_vals = ["1/86400", "1/604800"]

df = df.withColumn(
    "time_control_base_seconds",
    F.when(
        F.col("time_control_base_seconds").isNull() & (tc == F.lit("1/86400")),
        F.lit(86400)
    ).when(
        F.col("time_control_base_seconds").isNull() & (tc == F.lit("1/604800")),
        F.lit(604800)
    ).otherwise(F.col("time_control_base_seconds"))
)

df = df.withColumn(
    "time_control_increment_seconds",
    F.when(
        F.col("time_control_increment_seconds").isNull() &
        (tc.isin(daily_slash_vals)),
        F.lit(1)
    ).otherwise(F.col("time_control_increment_seconds"))
)

df = df.withColumn(
    "clock_remaining_pct",
    F.when(
        F.col("clock_remaining_pct").isNull() &
        (tc.isin(daily_slash_vals)) &
        F.col("side_to_move_clock_before").isNotNull() &
        (F.col("time_control_base_seconds") > 0),
        F.col("side_to_move_clock_before").cast("double") /
        F.col("time_control_base_seconds").cast("double")
    ).when(
        F.col("clock_remaining_pct").isNull() &
        (tc.isin(daily_slash_vals)) &
        (F.col("ply_index").isin([1, 2])),
        F.lit(1.0)
    ).otherwise(F.col("clock_remaining_pct"))
)


daily_slash = ["1/86400", "1/604800"]
tc = F.trim(F.col("time_control_raw"))
mask = tc.isin(daily_slash)

base_d = F.col("time_control_base_seconds").cast("double")
side_d = F.col("side_to_move_clock_before").cast("double")

side_filled = F.when(mask,
                     F.coalesce(side_d, base_d)
                    ).otherwise(side_d)

df = df.withColumn("side_to_move_clock_before", side_filled)

df = df.withColumn(
    "is_in_time_trouble_30s",
    F.when(
        mask & F.col("is_in_time_trouble_30s").isNull(),
        (F.col("side_to_move_clock_before") < F.lit(30)).cast("boolean")
    ).otherwise(F.col("is_in_time_trouble_30s"))
)

print("NULL side_to_move_clock_before (daily slash):",
      df.filter(mask & F.col("side_to_move_clock_before").isNull()).count())

print("NULL is_in_time_trouble_30s (daily slash):",
      df.filter(mask & F.col("is_in_time_trouble_30s").isNull()).count())


# ------------------------------
# 4. Feature selection
# ------------------------------
features = [
    "game_uuid",
    "rated",
    "rules",
    "time_class",
    "time_control_base_seconds",
    "time_control_increment_seconds",
    "white_rating",
    "black_rating",
    "rating_diff",
    "ply_index",
    "fullmove_number",
    "side_to_move",
    "is_capture",
    "is_check",
    "is_checkmate",
    "is_castling",
    "is_promotion",
    "piece_moved",
    "halfmove_clock_before",
    "legal_moves_count_before",
    "material_white_before",
    "material_black_before",
    "material_diff_before",
    "side_to_move_clock_before",
    "clock_remaining_pct",
    "avg_time_spent_per_move_so_far",
    "is_in_time_trouble_30s",
    "white_doubled_pawns_before",
    "black_doubled_pawns_before",
    "doubled_pawns_diff_before",
    "isolated_pawns_diff_before",
    "passed_pawns_diff_before",
    "white_can_castle_kingside_before",
    "white_can_castle_queenside_before",
    "black_can_castle_kingside_before",
    "black_can_castle_queenside_before",
    "in_check_before",
    "is_opening_phase",
    "is_middlegame_phase",
    "is_endgame_phase",
    "total_piece_count_before",
    "non_pawn_material_white_before",
    "bishops_pair_white_before",
    "bishops_pair_black_before",
    "final_result_class"
]

df_subset = df.select(*features)



label_col = "final_result_class"

cat_cols = ["rules", "time_class", "side_to_move", "piece_moved"]

bool_cols = [f.name for f in df_subset.schema.fields if isinstance(f.dataType, BooleanType)]

num_cols = [c for c in df_subset.columns if c not in cat_cols + bool_cols + [label_col] + ["game_uuid"]]

print("cat_cols:", cat_cols, '\n')
print("bool_cols:", bool_cols, '\n')
print("num_cols count:", num_cols)


# Type conversion (booleans and numeric)
for c in bool_cols:
    df_subset = df_subset.withColumn(c, F.col(c).cast("int").cast("double"))

for c in num_cols:
    df_subset = df_subset.withColumn(c, F.col(c).cast("double"))


# ------------------------------
# 5. Preprocessing pipeline
# ------------------------------
numeric_for_imputer = num_cols + bool_cols

imputed_cols = [c + "_imp" for c in numeric_for_imputer]

label_indexer = StringIndexer(
    inputCol=label_col, outputCol="label", handleInvalid="skip"
)

indexers = []
ohe = []
for c in cat_cols:
    idx_col = c + "_idx"
    ohe_col = c + "_ohe"
    indexers.append(
        StringIndexer(inputCol=c, outputCol=idx_col, handleInvalid="keep")
    )
    ohe.append(
        OneHotEncoder(inputCol=idx_col, outputCol=ohe_col)
    )

imputer = Imputer(
    strategy="median",
    inputCols=numeric_for_imputer,
    outputCols=imputed_cols
)

numeric_assembler = VectorAssembler(
    inputCols=imputed_cols,
    outputCol="numeric_features"
)

ohe_cols = [c + "_ohe" for c in cat_cols]

final_assembler = VectorAssembler(
    inputCols=["numeric_features"] + ohe_cols,
    outputCol="features"
)

preprocess = [label_indexer] + indexers + ohe + [imputer, numeric_assembler, final_assembler]


# ------------------------------
# 6. Train-test split
# ------------------------------
SEED = 42

games = df_subset.select("game_uuid").distinct().withColumn("r", F.rand(SEED))
train_games = games.filter(F.col("r") < 0.7).select("game_uuid")
test_games  = games.filter(F.col("r") >= 0.7).select("game_uuid")

train_df = df_subset.join(train_games, "game_uuid", "inner").drop("game_uuid")
test_df  = df_subset.join(test_games,  "game_uuid", "inner").drop("game_uuid")
print("Train rows:", train_df.count(), "Test rows:", test_df.count())


# ------------------------------
# 7. Prepare features
# ------------------------------
preprocess_pipeline = Pipeline(stages=preprocess)
preprocess_model = preprocess_pipeline.fit(train_df)
train_prepared = preprocess_model.transform(train_df).select("features", "label").persist(StorageLevel.MEMORY_AND_DISK)
test_prepared = preprocess_model.transform(test_df).select("features", "label").persist(StorageLevel.MEMORY_AND_DISK)
train_prepared.count()
test_prepared.count()

# ------------------------------
# 8. Evaluators
# ------------------------------
evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)

evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)


# ------------------------------
# 9. Random Forest (Modeling)
# ------------------------------
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    seed=42,
    featureSubsetStrategy="sqrt",
    subsamplingRate=0.8,
    maxBins=64
)

paramGrid_rf = (ParamGridBuilder()
    .addGrid(rf.numTrees, [20, 40, 60])             # 3
    .addGrid(rf.maxDepth, [3, 6, 9])              # 3
    .addGrid(rf.minInstancesPerNode, [5, 10, 20]) # 3  => 27
    .build()
)

cv_rf = CrossValidator(
    estimator=rf,
    estimatorParamMaps=paramGrid_rf,
    evaluator=evaluator_acc,
    numFolds=3,
    seed=42,
    parallelism=1
)

cvModelRf = cv_rf.fit(train_prepared)

best_rf_model = cvModelRf.bestModel
pred_rf = best_rf_model.transform(test_prepared)
acc_rf = evaluator_acc.evaluate(pred_rf)
f1_rf = evaluator_f1.evaluate(pred_rf)


print("RF -> TEST acc:", acc_rf, "TEST f1:", f1_rf)
print("Params:", best_rf_model.extractParamMap())


# ------------------------------
# 10. Naive Bayes (Modeling)
# ------------------------------
binarizer = Binarizer(
    inputCol="features",
    outputCol="features_bin",
    threshold=0.0
)

nb = NaiveBayes(
    featuresCol="features_bin",
    labelCol="label",
    predictionCol="prediction",
    modelType="bernoulli"
)

nb_pipeline = Pipeline(stages=[binarizer, nb])


paramGrid_nb = (ParamGridBuilder()
    .addGrid(nb.smoothing, [0.5, 1.0, 2.0])    # 3
    .addGrid(binarizer.threshold, [0.0, 0.5, 1.0])    # 3
    .addGrid(nb.modelType, ["bernoulli", "multinomial", "gaussian"])    # 3 => 27
    .build())


cv_nb = CrossValidator(
    estimator=nb_pipeline,
    estimatorParamMaps=paramGrid_nb,
    evaluator=evaluator_acc,
    numFolds=3,
    parallelism=1,
    seed=SEED
)

cvModelNb = cv_nb.fit(train_prepared)
best_nb_model = cvModelNb.bestModel

pred_nb = best_nb_model.transform(test_prepared)
acc_nb = evaluator_acc.evaluate(pred_nb)
f1_nb = evaluator_f1.evaluate(pred_nb)


print("Naive Bayes -> TEST acc:", acc_nb, "TEST f1:", f1_nb)
print("Params:", best_nb_model.extractParamMap())


# ------------------------------
# 11. Logistic Regression with scaler (Modeling)
# ------------------------------
spark.conf.set("spark.ml.crossValidator.parallelism", "1")


scaler = StandardScaler(inputCol="features", outputCol="features_scaled", withMean=False, withStd=True)
scaler_model = scaler.fit(train_prepared)
train_scaled = scaler_model.transform(train_prepared).select("features_scaled", "label") \
                     .persist(StorageLevel.MEMORY_AND_DISK)
test_scaled = scaler_model.transform(test_prepared).select("features_scaled", "label") \
                    .persist(StorageLevel.MEMORY_AND_DISK)
train_scaled.count()
test_scaled.count()

lr = LogisticRegression(
    featuresCol="features_scaled",
    labelCol="label",
    maxIter=50,
    family="multinomial",
    standardization=False
)

paramGrid_lr = (ParamGridBuilder()
    .addGrid(lr.regParam, [0.01, 0.1, 1.0])          # 3
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])    # 3
    .addGrid(lr.tol, [1e-4, 1e-3, 1e-2])            # 3 => 27
    .build())


cv_lr = CrossValidator(
    estimator=lr,
    estimatorParamMaps=paramGrid_lr,
    evaluator=evaluator_acc,
    numFolds=3,
    parallelism=1,
    seed=SEED
)


cvModelLr = cv_lr.fit(train_scaled)
best_lr_model = cvModelLr.bestModel

pred_lr = best_lr_model.transform(test_scaled)
acc_lr = evaluator_acc.evaluate(pred_lr)
f1_lr = evaluator_f1.evaluate(pred_lr)



print("Logistic Regression -> TEST acc:", acc_lr, "TEST f1:", f1_lr)
print("Params:", best_lr_model.extractParamMap())

train_prepared.unpersist()
test_prepared.unpersist()
train_scaled.unpersist()
test_scaled.unpersist()