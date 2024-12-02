### Detailed PySpark Task List for Data Engineering

#### **1. Data Loading and Exploration**
1.1. Load the dataset into a PySpark DataFrame from a CSV or Parquet file:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()
df = spark.read.csv("path/to/sales_data.csv", header=True, inferSchema=True)
```

1.2. Inspect the schema and data types:
```python
df.printSchema()
```

1.3. Perform basic exploratory analysis:
- Count rows and columns:
```python
df.count(), len(df.columns)
```
- Show sample rows:
```python
df.show(5)
```
- Summary statistics:
```python
df.describe().show()
```

#### **2. Data Cleaning**
2.1. Handle missing values:
- Replace nulls in numerical columns:
```python
df = df.fillna({"sales": 0, "quantity": 0})
```
- Replace nulls in categorical columns:
```python
df = df.fillna({"customer": "Unknown", "region": "Unknown"})
```

2.2. Drop rows with excessive missing values:
```python
df = df.dropna(thresh=3)  # Retain rows with at least 3 non-null values
```

2.3. Remove duplicates:
```python
df = df.dropDuplicates()
```

2.4. Fix data types:
```python
from pyspark.sql.types import TimestampType

df = df.withColumn("sales_date", df["sales_date"].cast(TimestampType()))
```

2.5. Standardize text data:
```python
from pyspark.sql.functions import trim, lower

df = df.withColumn("customer", trim(lower(df["customer"])))
```

#### **3. Feature Engineering**
3.1. Date transformations:
```python
from pyspark.sql.functions import year, month, dayofweek

df = df.withColumn("year", year(df["sales_date"])) \
         .withColumn("month", month(df["sales_date"])) \
         .withColumn("day_of_week", dayofweek(df["sales_date"]))
```

3.2. Customer segmentation:
```python
from pyspark.sql.functions import when

df = df.withColumn("customer_segment",
                   when(df["total_purchase"] > 1000, "High Value")
                   .when(df["total_purchase"] > 500, "Medium Value")
                   .otherwise("Low Value"))
```

3.3. Sales metrics:
```python
from pyspark.sql.functions import col

df = df.withColumn("profit", col("sales") - col("cost"))
```

#### **4. Data Aggregation**
4.1. Group by and aggregate:
```python
aggregated_df = df.groupBy("region", "product_category").agg(
    sum("sales").alias("total_sales"),
    avg("sales").alias("avg_sales"),
    max("sales").alias("max_sales")
)
```

4.2. Create a pivot table:
```python
pivot_df = df.groupBy("region").pivot("product_category").sum("sales")
```

#### **5. Data Enrichment**
5.1. Join with external datasets:
```python
demographics_df = spark.read.csv("path/to/demographics.csv", header=True, inferSchema=True)
df = df.join(demographics_df, on="region", how="left")
```

5.2. Add holiday flag:
```python
from pyspark.sql.functions import lit

holidays = ["2023-12-25", "2024-01-01"]  # Example holidays
df = df.withColumn("is_holiday", when(df["sales_date"].cast("string").isin(holidays), lit(1)).otherwise(lit(0)))
```

#### **6. Advanced Transformations**
6.1. Use window functions for ranking:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

windowSpec = Window.partitionBy("region").orderBy(col("total_sales").desc())
df = df.withColumn("rank", rank().over(windowSpec))
```

6.2. Calculate moving averages:
```python
from pyspark.sql.functions import avg

windowSpec = Window.partitionBy("region").orderBy("sales_date").rowsBetween(-2, 0)
df = df.withColumn("moving_avg", avg("sales").over(windowSpec))
```

#### **7. Data Quality Checks**
7.1. Check for outliers:
```python
from pyspark.sql.functions import approxQuantile

quantiles = df.approxQuantile("sales", [0.25, 0.75], 0.05)
IQR = quantiles[1] - quantiles[0]
lower_bound = quantiles[0] - 1.5 * IQR
upper_bound = quantiles[1] + 1.5 * IQR

outliers = df.filter((df["sales"] < lower_bound) | (df["sales"] > upper_bound))
outliers.show()
```

7.2. Validate consistency of categorical values:
```python
df.select("region").distinct().show()
```

#### **8. Output and Optimization**
8.1. Cache intermediate results:
```python
df.cache()
```

8.2. Save the transformed dataset:
```python
df.write.partitionBy("year", "month").parquet("path/to/output")
```

#### **9. Machine Learning Preparation (Optional)**
9.1. Normalize numerical features:
```python
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=["sales", "quantity"], outputCol="features")
assembled_df = assembler.transform(df)

scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(assembled_df)
scaled_df = scaler_model.transform(assembled_df)
```

9.2. Encode categorical variables:
```python
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="product_category", outputCol="product_category_index")
df = indexer.fit(df).transform(df)
```

#### **10. Reporting and Visualization Preparation**
- Save aggregated results for visualization tools:
```python
aggregated_df.write.csv("path/to/aggregated_sales.csv", header=True)
```

