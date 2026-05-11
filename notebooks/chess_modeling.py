# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PARQUET_PATH = "local_parquet/chess_moves_raw_parquet"

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet(PARQUET_PATH)

df.printSchema()
print("rows =", df.count())
df.select("final_result_class").groupBy("final_result_class").count().show()

# %%
def check_nulls(df):
    null_stats = (
        df.select([
            F.count(F.when(F.col(c).isNull(), 1)).alias(c)
            for c in df.columns
        ])
        .toPandas()
        .T
        .reset_index()
    )
    
    null_stats.columns = ["column", "null_count"]
    null_stats["null_pct"] = (null_stats["null_count"] / df.count()) * 100
    
    null_stats = null_stats[null_stats["null_count"] > 0].sort_values("null_pct", ascending=False)
    
    return null_stats

check_nulls(df)

# %%
# filling in "avg_time_spent_per_move_so_far"

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

# %% [markdown]
# Let's inspect *time_control_raw* values when *time_control_base_seconds* is Null

# %%
# find "time_control_raw" distribution when time_control_base_seconds is Null

df.filter(F.col("time_control_base_seconds").isNull()) \
  .select("time_control_raw","time_class","rated") \
  .groupBy("time_control_raw","time_class").count().show(50, False)

# %%
df.groupBy("time_class").count().show()

# %% [markdown]
# > We see that values of format A/B (for instance **1/604800**) belong to daily group of *time_class* feature and their quantity is 435. Also notice number of rows with *time_class* = **daily** is 435. So we can conclude that *time_control_raw* values are following the format of A/B when *time_class* is **daily**.
# 
# > Remember that *time_control_base_seconds* and *time_control_increment_seconds* were parsed from *time_control_raw* feature. It's easy to guess that A is time increment (in seconds) and B is time limit (for example 604800 seconds are 7 days).

# %%
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

# %%
check_nulls(df)

# %% [markdown]
# Let's also inspect values in *side_to_move_clock_before* and *is_in_time_trouble_30s*.

# %%
df.filter(F.col("side_to_move_clock_before").isNull()) \
  .select("time_control_raw", "side_to_move_clock_before", "is_in_time_trouble_30s", "clock_remaining_pct", "time_class", "ply_index").show()

# %% [markdown]
# From this example, it becomes clear that side_to_move_clock_before must be filled with values of *time_control_base_seconds* because *ply_index* values indicate that these rows represent first move of opponents in the game. Also obvious that *is_in_time_trouble_30s* must be False for all these rows.

# %%
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

# %%
check_nulls(df)

# %%
features = [
    "game_uuid",
    "rated",
    "rules",
    "time_class",
    "time_control_base_seconds",
    "time_control_increment_seconds",
    # "eco_code",
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
    # "promotion_piece",
    # "from_square",
    # "to_square",
    "piece_moved",
    # "en_passant_square_before",
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
    "pawn_shield_diff_before",
    "king_tropism_diff_before",
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

# %%
check_nulls(df_subset)

# %%
df_subset.printSchema()

# %%
df_subset.limit(10).toPandas()

# %%
from pyspark.sql.types import BooleanType

label_col = "final_result_class"

cat_cols = ["rules", "time_class", "side_to_move", "piece_moved"]

bool_cols = [f.name for f in df_subset.schema.fields if isinstance(f.dataType, BooleanType)]

num_cols = [c for c in df_subset.columns if c not in cat_cols + bool_cols + [label_col] + ["game_uuid"]]

print("cat_cols:", cat_cols, '\n')
print("bool_cols:", bool_cols, '\n')
print("num_cols count:", num_cols)

# %%
# Type conversion (booleans and numeric)
for c in bool_cols:
    df_subset = df_subset.withColumn(c, F.col(c).cast("int").cast("double"))

for c in num_cols:
    df_subset = df_subset.withColumn(c, F.col(c).cast("double"))

# %%
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, Imputer, VectorAssembler

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

# %%
SEED = 42

games = df_subset.select("game_uuid").distinct().withColumn("r", F.rand(SEED))
train_games = games.filter(F.col("r") < 0.7).select("game_uuid")
test_games  = games.filter(F.col("r") >= 0.7).select("game_uuid")

train_df = df_subset.join(train_games, "game_uuid", "inner")
test_df  = df_subset.join(test_games,  "game_uuid", "inner")

# %%
print("Train rows:", train_df.count(), "Test rows:", test_df.count())

# %% [markdown]
# # Now it is time for Modeling!

# %% [markdown]
# * ### Model 1 - Random Forest

# %%
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ===== 3) Random Forest + Grid Search (27 combinations) =====
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    seed=SEED
)

paramGrid_rf = (ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100, 200])           # 3
    .addGrid(rf.maxDepth, [5, 10, 20])             # 3
    .addGrid(rf.minInstancesPerNode, [1, 2, 5])    # 3  => 27
    .build()
)

evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)

# ---- best by Accuracy ----
rf_pipeline = Pipeline(stages=preprocess + [rf])

cv_acc = CrossValidator(
    estimator=rf_pipeline,
    estimatorParamMaps=paramGrid_rf,
    evaluator=evaluator_acc,
    numFolds=3,     # 2<k<5
    seed=SEED
)

cvModel_acc = cv_acc.fit(train_df)
best_rf_acc_model = cvModel_acc.bestModel

pred_acc = best_rf_acc_model.transform(test_df)
acc_test = evaluator_acc.evaluate(pred_acc)
f1_test  = evaluator_f1.evaluate(pred_acc)

print("RF BEST by Accuracy -> TEST Accuracy:", acc_test, "TEST F1:", f1_test)
print("Best RF params (by Accuracy):", best_rf_acc_model.stages[-1].extractParamMap())

# ---- best by F1 ----
cv_f1 = CrossValidator(
    estimator=rf_pipeline,
    estimatorParamMaps=paramGrid_rf,
    evaluator=evaluator_f1,
    numFolds=3,
    seed=SEED
)

cvModel_f1 = cv_f1.fit(train_df)
best_rf_f1_model = cvModel_f1.bestModel

pred_f1 = best_rf_f1_model.transform(test_df)
acc_test2 = evaluator_acc.evaluate(pred_f1)
f1_test2  = evaluator_f1.evaluate(pred_f1)

print("RF BEST by F1 -> TEST Accuracy:", acc_test2, "TEST F1:", f1_test2)
print("Best RF params (by F1):", best_rf_f1_model.stages[-1].extractParamMap())

# %% [markdown]
# * ### Model 2 - SVM (LinearSVC + OneVsRest)

# %%
from pyspark.ml.classification import LinearSVC, OneVsRest

SEED = 42
LABEL_COL = "final_result_class"

evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)

# Base classifier
svm = LinearSVC(featuresCol="features", labelCol="label", seed=SEED)

# Multiclass wrapper
ovr = OneVsRest(classifier=svm)

paramGrid_svm = (ParamGridBuilder()
    .addGrid(svm.regParam, [0.001, 0.01, 0.1])     # 3
    .addGrid(svm.maxIter, [20, 50, 100])         # 3
    .addGrid(svm.tol, [1e-4, 1e-3, 1e-2])        # 3 => 27
    .build()
)

svm_pipeline = Pipeline(stages=preprocess.getStages() + [ovr])

# --- best by Accuracy ---
cv_svm_acc = CrossValidator(
    estimator=svm_pipeline,
    estimatorParamMaps=paramGrid_svm,
    evaluator=evaluator_acc,
    numFolds=3,
    seed=SEED,
    parallelism=1
)

cvModel_svm_acc = cv_svm_acc.fit(train_df)
best_svm_acc_model = cvModel_svm_acc.bestModel

pred_svm_acc = best_svm_acc_model.transform(test_df)
acc_svm = evaluator_acc.evaluate(pred_svm_acc)
f1_svm = evaluator_f1.evaluate(pred_svm_acc)

print("SVM BEST by Accuracy -> TEST Accuracy:", acc_svm, "TEST F1:", f1_svm)
print("Best SVM params (by Accuracy):", best_svm_acc_model.stages[-1].extractParamMap())

# --- best by F1 ---
cv_svm_f1 = CrossValidator(
    estimator=svm_pipeline,
    estimatorParamMaps=paramGrid_svm,
    evaluator=evaluator_f1,
    numFolds=3,
    seed=SEED,
    parallelism=1
)

cvModel_svm_f1 = cv_svm_f1.fit(train_df)
best_svm_f1_model = cvModel_svm_f1.bestModel

pred_svm_f1 = best_svm_f1_model.transform(test_df)
acc_svm2 = evaluator_acc.evaluate(pred_svm_f1)
f1_svm2 = evaluator_f1.evaluate(pred_svm_f1)

print("SVM BEST by F1 -> TEST Accuracy:", acc_svm2, "TEST F1:", f1_svm2)
print("Best SVM params (by F1):", best_svm_f1_model.stages[-1].extractParamMap())

# %% [markdown]
# * ### Model 3 - Naive Bayes (Multinomial) 

# %%
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import MinMaxScaler, QuantileDiscretizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

SEED = 42
LABEL_COL = "final_result_class"

evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)

# scaler -> discretizer -> NB
scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

discretizer = QuantileDiscretizer(
    inputCol="scaledFeatures",
    outputCol="discFeatures",
    handleInvalid="skip"
)

nb = NaiveBayes(featuresCol="discFeatures", labelCol="label", modelType="multinomial")

# 27 комбинаций: 3 x 3 x 3
paramGrid_nb = (ParamGridBuilder()
    .addGrid(discretizer.numBuckets, [10, 20, 50])                 # 3
    .addGrid(discretizer.relativeError, [0.01, 0.05, 0.1])      # 3
    .addGrid(nb.smoothing, [0.0, 0.5, 1.0])                      # 3 => 27
    .build()
)

nb_pipeline = Pipeline(stages=preprocess.getStages() + [scaler, discretizer, nb])

# --- best by Accuracy ---
cv_nb_acc = CrossValidator(
    estimator=nb_pipeline,
    estimatorParamMaps=paramGrid_nb,
    evaluator=evaluator_acc,
    numFolds=3,
    seed=SEED,
    parallelism=1
)

cvModel_nb_acc = cv_nb_acc.fit(train_df)
best_nb_acc_model = cvModel_nb_acc.bestModel

pred_nb_acc = best_nb_acc_model.transform(test_df)
acc_nb = evaluator_acc.evaluate(pred_nb_acc)
f1_nb = evaluator_f1.evaluate(pred_nb_acc)

print("NB BEST by Accuracy -> TEST Accuracy:", acc_nb, "TEST F1:", f1_nb)
print("Best NB params (by Accuracy):", best_nb_acc_model.stages[-1].extractParamMap())

# --- best by F1 ---
cv_nb_f1 = CrossValidator(
    estimator=nb_pipeline,
    estimatorParamMaps=paramGrid_nb,
    evaluator=evaluator_f1,
    numFolds=3,
    seed=SEED,
    parallelism=1
)

cvModel_nb_f1 = cv_nb_f1.fit(train_df)
best_nb_f1_model = cvModel_nb_f1.bestModel

pred_nb_f1 = best_nb_f1_model.transform(test_df)
acc_nb2 = evaluator_acc.evaluate(pred_nb_f1)
f1_nb2 = evaluator_f1.evaluate(pred_nb_f1)

print("NB BEST by F1 -> TEST Accuracy:", acc_nb2, "TEST F1:", f1_nb2)
print("Best NB params (by F1):", best_nb_f1_model.stages[-1].extractParamMap())

# %%


# %%



