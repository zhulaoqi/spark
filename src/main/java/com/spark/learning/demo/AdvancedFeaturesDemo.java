package com.spark.learning.demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Spark 高级特性演示
 * 
 * 包括：广播变量、累加器、窗口函数、缓存和持久化等
 */
public class AdvancedFeaturesDemo {

    /**
     * 销售记录类
     */
    public static class SalesRecord implements Serializable {
        private String date;
        private String product;
        private String region;
        private int quantity;
        private double amount;

        public SalesRecord() {}

        public SalesRecord(String date, String product, String region, int quantity, double amount) {
            this.date = date;
            this.product = product;
            this.region = region;
            this.quantity = quantity;
            this.amount = amount;
        }

        public String getDate() { return date; }
        public void setDate(String date) { this.date = date; }
        public String getProduct() { return product; }
        public void setProduct(String product) { this.product = product; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    /**
     * 演示广播变量
     * 广播变量允许程序员将一个只读变量缓存到每台机器上，而不是为每个任务发送一个副本
     */
    public static void demonstrateBroadcastVariable(JavaSparkContext sc) {
        System.out.println("\n========== 广播变量演示 ==========");
        
        // 创建一个较大的查找表（在实际应用中可能是数百MB或更大）
        Map<String, String> productNames = new HashMap<>();
        productNames.put("P001", "iPhone 15");
        productNames.put("P002", "MacBook Pro");
        productNames.put("P003", "iPad Air");
        productNames.put("P004", "AirPods Pro");
        
        // 创建广播变量
        Broadcast<Map<String, String>> broadcastProductNames = sc.broadcast(productNames);
        
        // 模拟产品ID数据
        List<String> productIds = Arrays.asList("P001", "P002", "P003", "P001", "P004");
        JavaRDD<String> productRDD = sc.parallelize(productIds);
        
        // 在 map 操作中使用广播变量
        JavaRDD<String> productFullNames = productRDD.map(id -> {
            // 获取广播变量的值
            Map<String, String> names = broadcastProductNames.value();
            return id + ": " + names.getOrDefault(id, "Unknown");
        });
        
        System.out.println("使用广播变量转换产品ID为名称:");
        productFullNames.collect().forEach(p-> System.out.println(p.toString()));
        
        // 使用完毕后可以销毁广播变量
        broadcastProductNames.unpersist();
        System.out.println("广播变量已释放");
    }

    /**
     * 演示累加器
     * 累加器是用于实现计数器或求和的变量，只能通过关联和交换操作"添加"
     */
    public static void demonstrateAccumulator(JavaSparkContext sc) {
        System.out.println("\n========== 累加器演示 ==========");
        
        // 创建累加器
        LongAccumulator errorCounter = sc.sc().longAccumulator("errorCounter");
        LongAccumulator validCounter = sc.sc().longAccumulator("validCounter");
        
        // 模拟数据处理
        List<Integer> numbers = Arrays.asList(1, 2, 0, 4, 0, 6, 7, 0, 9, 10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        
        // 在 foreach 中使用累加器
        JavaRDD<Double> results = numbersRDD.map(num -> {
            if (num == 0) {
                errorCounter.add(1);
                return 0.0;
            } else {
                validCounter.add(1);
                return 100.0 / num;
            }
        });
        
        // 触发计算
        results.collect();
        
        System.out.println("有效数据数量: " + validCounter.value());
        System.out.println("错误数据数量: " + errorCounter.value());
        
        // 自定义累加器示例
        LongAccumulator sumAccumulator = sc.sc().longAccumulator("sumAccumulator");
        numbersRDD.foreach(num -> sumAccumulator.add(num));
        System.out.println("所有数字的和: " + sumAccumulator.value());
    }

    /**
     * 演示窗口函数（使用 DataFrame API）
     */
    public static void demonstrateWindowFunctions(SparkSession spark) {
        System.out.println("\n========== 窗口函数演示 ==========");
        
        List<SalesRecord> sales = Arrays.asList(
            new SalesRecord("2024-01-01", "iPhone", "North", 10, 9999.0),
            new SalesRecord("2024-01-02", "iPhone", "North", 15, 14998.5),
            new SalesRecord("2024-01-01", "MacBook", "North", 5, 9999.5),
            new SalesRecord("2024-01-02", "MacBook", "North", 8, 15999.2),
            new SalesRecord("2024-01-01", "iPhone", "South", 12, 11998.8),
            new SalesRecord("2024-01-02", "iPhone", "South", 20, 19998.0),
            new SalesRecord("2024-01-01", "MacBook", "South", 6, 11999.4),
            new SalesRecord("2024-01-02", "MacBook", "South", 10, 19999.0)
        );
        
        Dataset<Row> df = spark.createDataFrame(sales, SalesRecord.class);
        
        // 1. 按产品和地区分区，按日期排序的窗口
        WindowSpec windowSpec = Window.partitionBy("product", "region").orderBy("date");
        
        System.out.println("窗口函数 - 累计销售额:");
        df.withColumn("cumulative_amount", sum("amount").over(windowSpec))
          .show();
        
        // 2. 排名函数
        WindowSpec rankWindow = Window.partitionBy("region").orderBy(col("amount").desc());
        
        System.out.println("窗口函数 - 排名:");
        df.withColumn("rank", rank().over(rankWindow))
          .withColumn("dense_rank", dense_rank().over(rankWindow))
          .withColumn("row_number", row_number().over(rankWindow))
          .show();
        
        // 3. 移动平均
        WindowSpec movingAvgWindow = Window.partitionBy("product")
                                            .orderBy("date")
                                            .rowsBetween(-1, 1);
        
        System.out.println("窗口函数 - 移动平均:");
        df.withColumn("moving_avg_quantity", avg("quantity").over(movingAvgWindow))
          .show();
        
        // 4. LAG 和 LEAD 函数
        System.out.println("窗口函数 - LAG 和 LEAD:");
        df.withColumn("prev_amount", lag("amount", 1).over(windowSpec))
          .withColumn("next_amount", lead("amount", 1).over(windowSpec))
          .show();
        
        // 5. 分组内的百分比
        WindowSpec percentWindow = Window.partitionBy("region");
        
        System.out.println("窗口函数 - 分组内百分比:");
        df.withColumn("total_amount", sum("amount").over(percentWindow))
          .withColumn("percentage", round(col("amount").multiply(100).divide(col("total_amount")), 2))
          .show();
    }

    /**
     * 演示缓存和持久化
     */
    public static void demonstrateCachingAndPersistence(SparkSession spark) {
        System.out.println("\n========== 缓存和持久化演示 ==========");
        
        List<SalesRecord> sales = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            sales.add(new SalesRecord(
                "2024-01-" + String.format("%02d", (i % 30) + 1),
                "Product" + (i % 10),
                "Region" + (i % 5),
                i % 100,
                i * 10.0
            ));
        }
        
        Dataset<Row> df = spark.createDataFrame(sales, SalesRecord.class);
        
        // 1. 不使用缓存
        System.out.println("不使用缓存 - 第一次计算:");
        long start1 = System.currentTimeMillis();
        long count1 = df.filter(col("amount").gt(5000)).count();
        long time1 = System.currentTimeMillis() - start1;
        System.out.println("结果: " + count1 + ", 耗时: " + time1 + "ms");
        
        System.out.println("不使用缓存 - 第二次计算:");
        long start2 = System.currentTimeMillis();
        long count2 = df.filter(col("amount").gt(5000)).count();
        long time2 = System.currentTimeMillis() - start2;
        System.out.println("结果: " + count2 + ", 耗时: " + time2 + "ms");
        
        // 2. 使用缓存
        Dataset<Row> cachedDF = df.cache();
        
        System.out.println("使用缓存 - 第一次计算:");
        long start3 = System.currentTimeMillis();
        long count3 = cachedDF.filter(col("amount").gt(5000)).count();
        long time3 = System.currentTimeMillis() - start3;
        System.out.println("结果: " + count3 + ", 耗时: " + time3 + "ms");
        
        System.out.println("使用缓存 - 第二次计算 (从缓存读取):");
        long start4 = System.currentTimeMillis();
        long count4 = cachedDF.filter(col("amount").gt(5000)).count();
        long time4 = System.currentTimeMillis() - start4;
        System.out.println("结果: " + count4 + ", 耗时: " + time4 + "ms");
        
        // 3. 不同的持久化级别
        System.out.println("\n不同的持久化级别:");
        System.out.println("- MEMORY_ONLY: 只存储在内存中");
        System.out.println("- MEMORY_AND_DISK: 内存不足时溢出到磁盘");
        System.out.println("- MEMORY_ONLY_SER: 以序列化形式存储在内存");
        System.out.println("- DISK_ONLY: 只存储在磁盘");
        
        // 4. 释放缓存
        cachedDF.unpersist();
        System.out.println("\n缓存已释放");
    }

    /**
     * 演示分区操作
     */
    public static void demonstratePartitioning(SparkSession spark) {
        System.out.println("\n========== 分区操作演示 ==========");
        
        List<SalesRecord> sales = Arrays.asList(
            new SalesRecord("2024-01-01", "iPhone", "North", 10, 9999.0),
            new SalesRecord("2024-01-02", "iPhone", "South", 15, 14998.5),
            new SalesRecord("2024-01-01", "MacBook", "East", 5, 9999.5),
            new SalesRecord("2024-01-02", "MacBook", "West", 8, 15999.2)
        );
        
        Dataset<Row> df = spark.createDataFrame(sales, SalesRecord.class);
        
        System.out.println("原始分区数: " + df.rdd().getNumPartitions());
        
        // 1. repartition: 增加或减少分区（会发生 shuffle）
        Dataset<Row> repartitioned = df.repartition(10);
        System.out.println("repartition 后分区数: " + repartitioned.rdd().getNumPartitions());
        
        // 2. coalesce: 减少分区（尽量避免 shuffle）
        Dataset<Row> coalesced = repartitioned.coalesce(2);
        System.out.println("coalesce 后分区数: " + coalesced.rdd().getNumPartitions());
        
        // 3. 按列重新分区
        Dataset<Row> repartitionedByColumn = df.repartition(4, col("region"));
        System.out.println("按 region 列重新分区后: " + repartitionedByColumn.rdd().getNumPartitions());
        
        // 4. 查看分区数据分布
        System.out.println("每个分区的记录数:");
        JavaRDD<Row> javaRDD = repartitionedByColumn.toJavaRDD();
        javaRDD.mapPartitionsWithIndex((index, iter) -> {
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            List<Tuple2<Integer, Integer>> result = new ArrayList<>();
            result.add(new Tuple2<>(index, count));
            return result.iterator();
        }, true).collect().forEach(t -> System.out.println("分区 " + t._1 + ": " + t._2 + " 条记录"));
    }

    /**
     * 演示数据倾斜处理
     */
    public static void demonstrateDataSkewHandling(SparkSession spark) {
        System.out.println("\n========== 数据倾斜处理演示 ==========");
        
        // 创建一个有数据倾斜的数据集
        List<SalesRecord> sales = new ArrayList<>();
        // 大量 Region A 的数据
        for (int i = 0; i < 1000; i++) {
            sales.add(new SalesRecord("2024-01-01", "iPhone", "RegionA", i, i * 10.0));
        }
        // 少量其他 Region 的数据
        for (int i = 0; i < 10; i++) {
            sales.add(new SalesRecord("2024-01-01", "iPhone", "RegionB", i, i * 10.0));
            sales.add(new SalesRecord("2024-01-01", "iPhone", "RegionC", i, i * 10.0));
        }
        
        Dataset<Row> df = spark.createDataFrame(sales, SalesRecord.class);
        
        System.out.println("数据分布:");
        df.groupBy("region").count().show();
        
        // 处理方法1: 加盐 (Salting)
        System.out.println("方法1 - 加盐处理:");
        Dataset<Row> saltedDF = df.withColumn("salt", (rand().multiply(10)).cast("int"))
                                  .withColumn("salted_region", concat(col("region"), lit("_"), col("salt")));
        
        System.out.println("加盐后的数据分布:");
        saltedDF.groupBy("salted_region").count().show();
        
        // 处理方法2: 两阶段聚合
        System.out.println("方法2 - 两阶段聚合:");
        // 第一阶段：加盐聚合
        Dataset<Row> stage1 = saltedDF.groupBy("salted_region", "product")
                                      .agg(sum("amount").alias("partial_sum"));
        
        // 第二阶段：去盐聚合
        Dataset<Row> stage2 = stage1.withColumn("original_region", 
                                                regexp_replace(col("salted_region"), "_\\d+$", ""))
                                    .groupBy("original_region", "product")
                                    .agg(sum("partial_sum").alias("total_amount"));
        
        System.out.println("两阶段聚合结果:");
        stage2.show();
    }

    /**
     * 演示自定义分区器
     */
    public static void demonstrateCustomPartitioner(JavaSparkContext sc) {
        System.out.println("\n========== 自定义分区器演示 ==========");
        
        // 创建键值对数据
        List<Tuple2<String, Integer>> data = Arrays.asList(
            new Tuple2<>("A1", 1),
            new Tuple2<>("A2", 2),
            new Tuple2<>("B1", 3),
            new Tuple2<>("B2", 4),
            new Tuple2<>("C1", 5),
            new Tuple2<>("C2", 6)
        );
        
        var pairRDD = sc.parallelizePairs(data, 2);
        
        System.out.println("默认分区:");
        System.out.println("分区数: " + pairRDD.getNumPartitions());
        
        // 使用 HashPartitioner
        var hashPartitioned = pairRDD.partitionBy(new org.apache.spark.HashPartitioner(3));
        System.out.println("HashPartitioner 后分区数: " + hashPartitioned.getNumPartitions());
        
        System.out.println("自定义分区器可以根据业务逻辑将数据分配到不同的分区");
    }

    /**
     * 运行所有演示
     */
    public static void runAllDemos(SparkSession spark, JavaSparkContext sc) {
        demonstrateBroadcastVariable(sc);
        demonstrateAccumulator(sc);
        demonstrateWindowFunctions(spark);
        demonstrateCachingAndPersistence(spark);
        demonstratePartitioning(spark);
        demonstrateDataSkewHandling(spark);
        demonstrateCustomPartitioner(sc);
    }

    /**
     * 独立运行入口
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Advanced Features Demo")
            .config("spark.master", "local[*]")
            .config("spark.driver.extraJavaOptions", 
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED")
            .config("spark.executor.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED")
            .getOrCreate();
        
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        spark.sparkContext().setLogLevel("WARN");
        
        System.out.println("========== 高级特性演示开始 ==========");
        runAllDemos(spark, sc);
        System.out.println("========== 高级特性演示完成 ==========");
        
        spark.stop();
    }
}

