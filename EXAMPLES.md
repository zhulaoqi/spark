# Spark 学习示例详解

## 目录

1. [RDD 操作示例](#rdd-操作示例)
2. [DataFrame 操作示例](#dataframe-操作示例)
3. [Spark SQL 示例](#spark-sql-示例)
4. [数据读写示例](#数据读写示例)
5. [性能优化示例](#性能优化示例)

---

## RDD 操作示例

### 1. 词频统计（Word Count）

最经典的 Spark 示例：

```java
JavaRDD<String> lines = sc.textFile("data/input/sample.txt");

JavaRDD<String> words = lines.flatMap(line -> 
    Arrays.asList(line.split(" ")).iterator()
);

JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> 
    new Tuple2<>(word.toLowerCase(), 1)
);

JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

// 按词频降序排序
List<Tuple2<String, Integer>> sortedCounts = wordCounts
    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
    .sortByKey(false)
    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
    .collect();

sortedCounts.forEach(tuple -> 
    System.out.println(tuple._1 + ": " + tuple._2)
);
```

### 2. 数据过滤和转换

```java
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
JavaRDD<Integer> rdd = sc.parallelize(numbers);

// 过滤偶数，平方后求和
int result = rdd
    .filter(x -> x % 2 == 0)
    .map(x -> x * x)
    .reduce(Integer::sum);

System.out.println("偶数平方和: " + result);
// 输出: 220 (4 + 16 + 36 + 64 + 100)
```

### 3. Join 操作

```java
// 用户数据
List<Tuple2<Integer, String>> users = Arrays.asList(
    new Tuple2<>(1, "Alice"),
    new Tuple2<>(2, "Bob"),
    new Tuple2<>(3, "Charlie")
);

// 订单数据
List<Tuple2<Integer, Double>> orders = Arrays.asList(
    new Tuple2<>(1, 100.0),
    new Tuple2<>(2, 200.0),
    new Tuple2<>(1, 150.0)
);

JavaPairRDD<Integer, String> usersRDD = sc.parallelizePairs(users);
JavaPairRDD<Integer, Double> ordersRDD = sc.parallelizePairs(orders);

// Join
JavaPairRDD<Integer, Tuple2<String, Double>> joined = usersRDD.join(ordersRDD);

joined.collect().forEach(System.out::println);
// 输出:
// (1, (Alice, 100.0))
// (1, (Alice, 150.0))
// (2, (Bob, 200.0))
```

---

## DataFrame 操作示例

### 1. 数据分析

```java
// 读取 CSV 文件
Dataset<Row> df = spark.read()
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/input/products.csv");

// 基本统计
df.describe("price", "stock").show();

// 按类别统计
df.groupBy("category")
  .agg(
      count("*").alias("count"),
      avg("price").alias("avg_price"),
      sum("stock").alias("total_stock")
  )
  .orderBy(col("count").desc())
  .show();
```

### 2. 复杂查询

```java
// 查找高价值产品（价格 > 平均价格）
double avgPrice = df.agg(avg("price")).first().getDouble(0);

Dataset<Row> highValueProducts = df
    .filter(col("price").gt(avgPrice))
    .select("name", "category", "price")
    .orderBy(col("price").desc());

highValueProducts.show();
```

### 3. 数据转换

```java
// 添加计算列
Dataset<Row> enriched = df
    .withColumn("total_value", col("price").multiply(col("stock")))
    .withColumn("price_level", 
        when(col("price").lt(100), "Low")
        .when(col("price").lt(500), "Medium")
        .otherwise("High")
    );

enriched.show();
```

---

## Spark SQL 示例

### 1. 复杂聚合查询

```java
df.createOrReplaceTempView("products");

String sql = """
    SELECT 
        category,
        COUNT(*) as product_count,
        ROUND(AVG(price), 2) as avg_price,
        MAX(price) as max_price,
        MIN(price) as min_price,
        SUM(stock * price) as inventory_value
    FROM products
    GROUP BY category
    ORDER BY inventory_value DESC
    """;

Dataset<Row> result = spark.sql(sql);
result.show();
```

### 2. 窗口函数查询

```java
String sql = """
    SELECT 
        name,
        category,
        price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rank,
        ROUND(AVG(price) OVER (PARTITION BY category), 2) as category_avg_price,
        price - AVG(price) OVER (PARTITION BY category) as price_diff
    FROM products
    """;

spark.sql(sql).show();
```

### 3. 自连接查询

```java
String sql = """
    SELECT 
        p1.name as product1,
        p2.name as product2,
        p1.category,
        ABS(p1.price - p2.price) as price_diff
    FROM products p1
    JOIN products p2 
        ON p1.category = p2.category 
        AND p1.id < p2.id
    WHERE ABS(p1.price - p2.price) < 100
    ORDER BY p1.category, price_diff
    """;

spark.sql(sql).show();
```

---

## 数据读写示例

### 1. 多格式读写

```java
// 读取 CSV
Dataset<Row> csv = spark.read()
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/input/products.csv");

// 写入 Parquet
csv.write()
   .mode(SaveMode.Overwrite)
   .parquet("data/output/products.parquet");

// 读取 Parquet
Dataset<Row> parquet = spark.read()
    .parquet("data/output/products.parquet");

// 写入 JSON
parquet.write()
       .mode(SaveMode.Overwrite)
       .json("data/output/products.json");
```

### 2. 分区写入

```java
// 按类别分区
df.write()
  .mode(SaveMode.Overwrite)
  .partitionBy("category")
  .parquet("data/output/products_partitioned");

// 读取特定分区
Dataset<Row> electronics = spark.read()
    .parquet("data/output/products_partitioned/category=Electronics");

electronics.show();
```

### 3. 动态分区覆盖

```java
spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");

// 只覆盖匹配的分区
df.filter(col("category").equalTo("Electronics"))
  .write()
  .mode(SaveMode.Overwrite)
  .partitionBy("category")
  .parquet("data/output/products_partitioned");
```

---

## 性能优化示例

### 1. 缓存优化

```java
// 场景：需要多次使用同一个 DataFrame
Dataset<Row> expensiveDF = spark.read()
    .parquet("large_dataset.parquet")
    .filter(col("date").gt("2024-01-01"))
    .cache();  // 缓存

// 第一次使用（触发缓存）
long count1 = expensiveDF.filter(col("amount").gt(1000)).count();

// 第二次使用（从缓存读取，快速）
long count2 = expensiveDF.filter(col("amount").lt(100)).count();

// 使用完毕释放缓存
expensiveDF.unpersist();
```

### 2. 广播 Join

```java
// 小表
Dataset<Row> smallTable = spark.read().csv("small.csv");

// 大表
Dataset<Row> largeTable = spark.read().parquet("large.parquet");

// 使用广播 Join（避免 Shuffle）
Dataset<Row> result = largeTable.join(
    broadcast(smallTable),
    "key"
);
```

### 3. 分区优化

```java
// 数据倾斜处理：加盐
Dataset<Row> skewed = df
    .withColumn("salt", (rand().multiply(10)).cast("int"))
    .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")));

// 第一阶段聚合
Dataset<Row> stage1 = skewed
    .groupBy("salted_key")
    .agg(sum("value").alias("partial_sum"));

// 第二阶段聚合
Dataset<Row> stage2 = stage1
    .withColumn("original_key", regexp_replace(col("salted_key"), "_\\d+$", ""))
    .groupBy("original_key")
    .agg(sum("partial_sum").alias("total_sum"));
```

### 4. 列式存储优化

```java
// 使用 Parquet 格式（列式存储）
df.write()
  .option("compression", "snappy")  // 压缩
  .parquet("output.parquet");

// 只读取需要的列
Dataset<Row> selected = spark.read()
    .parquet("output.parquet")
    .select("id", "name", "price");  // 列裁剪
```

### 5. 谓词下推

```java
// Spark 会自动将过滤条件下推到数据源
Dataset<Row> filtered = spark.read()
    .parquet("data.parquet")
    .filter(col("date").gt("2024-01-01"))  // 谓词下推
    .filter(col("amount").gt(1000));       // 谓词下推

// 只读取满足条件的数据块，减少 I/O
```

---

## 实战场景

### 场景 1: 日志分析

```java
// 读取日志文件
JavaRDD<String> logs = sc.textFile("logs/*.log");

// 解析日志
JavaRDD<LogEntry> parsed = logs.map(line -> parseLog(line));

// 统计错误
Map<String, Long> errorCounts = parsed
    .filter(log -> log.level.equals("ERROR"))
    .mapToPair(log -> new Tuple2<>(log.message, 1L))
    .reduceByKey(Long::sum)
    .collectAsMap();
```

### 场景 2: 用户行为分析

```java
// 用户行为数据
Dataset<Row> events = spark.read().json("events.json");

// 计算用户留存率
Dataset<Row> retention = events
    .groupBy("user_id", "date")
    .agg(count("*").alias("events"))
    .withColumn("retention_day", 
        datediff(col("date"), lit("2024-01-01")))
    .groupBy("retention_day")
    .agg(countDistinct("user_id").alias("users"));
```

### 场景 3: 销售报表

```java
String sql = """
    WITH daily_sales AS (
        SELECT 
            DATE(order_time) as date,
            product_id,
            SUM(quantity) as daily_quantity,
            SUM(amount) as daily_revenue
        FROM orders
        GROUP BY DATE(order_time), product_id
    )
    SELECT 
        date,
        product_id,
        daily_revenue,
        SUM(daily_revenue) OVER (
            PARTITION BY product_id 
            ORDER BY date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_revenue,
        AVG(daily_revenue) OVER (
            PARTITION BY product_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as ma_7day_revenue
    FROM daily_sales
    ORDER BY product_id, date
    """;

spark.sql(sql).show();
```

---

## 总结

这些示例涵盖了 Spark 的主要使用场景和最佳实践。建议：

1. **从简单开始**: 先掌握基本的 RDD 和 DataFrame 操作
2. **理解原理**: 了解 Shuffle、分区、缓存等概念
3. **性能优化**: 学习使用缓存、广播、分区等优化技术
4. **实践应用**: 将示例应用到实际项目中

更多详细信息，请参考 `README.md` 和源代码注释。

