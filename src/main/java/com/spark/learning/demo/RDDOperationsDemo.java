package com.spark.learning.demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * RDD (弹性分布式数据集) 操作演示
 * 
 * RDD 是 Spark 的基础数据结构，具有以下特性：
 * 1. 分布式：数据分布在集群的多个节点上
 * 2. 不可变：一旦创建就不能修改
 * 3. 弹性：可以自动从故障中恢复
 * 4. 分区：数据被分成多个分区进行并行处理
 */
public class RDDOperationsDemo {

    /**
     * 演示 RDD 的创建方式
     */
    public static void demonstrateRDDCreation(JavaSparkContext sc) {
        System.out.println("\n========== RDD 创建演示 ==========");
        
        // 方式1: 从集合创建
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd1 = sc.parallelize(numbers);
        System.out.println("从集合创建的 RDD: " + rdd1.collect());
        
        // 方式2: 从文本文件创建（示例）
        // JavaRDD<String> rdd2 = sc.textFile("data/input.txt");
        
        // 方式3: 指定分区数创建
        JavaRDD<Integer> rdd3 = sc.parallelize(numbers, 4);
        System.out.println("分区数: " + rdd3.getNumPartitions());
    }

    /**
     * 演示转换操作 (Transformation)
     * 转换操作是惰性求值的，不会立即执行
     */
    public static void demonstrateTransformations(JavaSparkContext sc) {
        System.out.println("\n========== RDD 转换操作演示 ==========");
        
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = sc.parallelize(numbers);
        
        // 1. map: 对每个元素应用函数
        JavaRDD<Integer> mapped = rdd.map(x -> x * 2);
        System.out.println("map (x * 2): " + mapped.collect());
        
        // 2. filter: 过滤元素
        JavaRDD<Integer> filtered = rdd.filter(x -> x % 2 == 0);
        System.out.println("filter (偶数): " + filtered.collect());
        
        // 3. flatMap: 将每个元素映射为多个元素
        JavaRDD<Integer> flatMapped = rdd.flatMap(x -> Arrays.asList(x, x * 2).iterator());
        System.out.println("flatMap (x, x*2): " + flatMapped.collect());
        
        // 4. distinct: 去重
        JavaRDD<Integer> withDuplicates = sc.parallelize(Arrays.asList(1, 2, 2, 3, 3, 3));
        JavaRDD<Integer> distinct = withDuplicates.distinct();
        System.out.println("distinct: " + distinct.collect());
        
        // 5. sample: 采样
        JavaRDD<Integer> sample = rdd.sample(false, 0.5, 42L);
        System.out.println("sample (50%): " + sample.collect());
        
        // 6. union: 合并两个 RDD
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(4, 5, 6));
        JavaRDD<Integer> union = rdd1.union(rdd2);
        System.out.println("union: " + union.collect());
        
        // 7. intersection: 交集
        JavaRDD<Integer> rdd3 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> rdd4 = sc.parallelize(Arrays.asList(3, 4, 5, 6));
        JavaRDD<Integer> intersection = rdd3.intersection(rdd4);
        System.out.println("intersection: " + intersection.collect());
        
        // 8. subtract: 差集
        JavaRDD<Integer> subtract = rdd3.subtract(rdd4);
        System.out.println("subtract (rdd3 - rdd4): " + subtract.collect());
    }

    /**
     * 演示行动操作 (Action)
     * 行动操作会触发实际的计算
     */
    public static void demonstrateActions(JavaSparkContext sc) {
        System.out.println("\n========== RDD 行动操作演示 ==========");
        
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = sc.parallelize(numbers);
        
        // 1. collect: 收集所有元素到驱动程序
        List<Integer> collected = rdd.collect();
        System.out.println("collect: " + collected);
        
        // 2. count: 计数
        long count = rdd.count();
        System.out.println("count: " + count);
        
        // 3. first: 获取第一个元素
        Integer first = rdd.first();
        System.out.println("first: " + first);
        
        // 4. take: 获取前 N 个元素
        List<Integer> taken = rdd.take(5);
        System.out.println("take(5): " + taken);
        
        // 5. reduce: 聚合元素
        Integer sum = rdd.reduce(Integer::sum);
        System.out.println("reduce (sum): " + sum);
        
        // 6. fold: 带初始值的聚合
        Integer folded = rdd.fold(0, Integer::sum);
        System.out.println("fold (sum with 0): " + folded);
        
        // 7. aggregate: 更灵活的聚合
        Integer aggregated = rdd.aggregate(
            0,
            Integer::sum,  // 分区内聚合
            Integer::sum   // 分区间聚合
        );
        System.out.println("aggregate (sum): " + aggregated);
        
        // 8. foreach: 对每个元素执行操作
        System.out.print("foreach: ");
        rdd.foreach(x -> System.out.print(x + " "));
        System.out.println();
        
        // 9. countByValue: 统计每个值的出现次数
        JavaRDD<Integer> withDuplicates = sc.parallelize(Arrays.asList(1, 2, 2, 3, 3, 3));
        Map<Integer, Long> countByValue = withDuplicates.countByValue();
        System.out.println("countByValue: " + countByValue);
    }

    /**
     * 演示 Pair RDD 操作（键值对 RDD）
     */
    public static void demonstratePairRDDOperations(JavaSparkContext sc) {
        System.out.println("\n========== Pair RDD 操作演示 ==========");
        
        // 创建 Pair RDD
        List<Tuple2<String, Integer>> data = Arrays.asList(
            new Tuple2<>("apple", 3),
            new Tuple2<>("banana", 2),
            new Tuple2<>("apple", 5),
            new Tuple2<>("orange", 4),
            new Tuple2<>("banana", 1)
        );
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
        
        // 1. reduceByKey: 按键聚合
        JavaPairRDD<String, Integer> reduced = pairRDD.reduceByKey(Integer::sum);
        System.out.println("reduceByKey (sum): " + reduced.collect());
        
        // 2. groupByKey: 按键分组
        JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey();
        System.out.println("groupByKey: " + grouped.collect());
        
        // 3. mapValues: 只对值进行映射
        JavaPairRDD<String, Integer> mappedValues = pairRDD.mapValues(v -> v * 10);
        System.out.println("mapValues (v * 10): " + mappedValues.collect());
        
        // 4. keys: 获取所有键
        JavaRDD<String> keys = pairRDD.keys();
        System.out.println("keys: " + keys.collect());
        
        // 5. values: 获取所有值
        JavaRDD<Integer> values = pairRDD.values();
        System.out.println("values: " + values.collect());
        
        // 6. sortByKey: 按键排序
        JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey();
        System.out.println("sortByKey: " + sorted.collect());
        
        // 7. join: 连接两个 Pair RDD
        List<Tuple2<String, String>> data2 = Arrays.asList(
            new Tuple2<>("apple", "red"),
            new Tuple2<>("banana", "yellow"),
            new Tuple2<>("orange", "orange")
        );
        JavaPairRDD<String, String> pairRDD2 = sc.parallelizePairs(data2);
        JavaPairRDD<String, Tuple2<Integer, String>> joined = pairRDD.join(pairRDD2);
        System.out.println("join: " + joined.collect());
        
        // 8. cogroup: 协同分组
        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<String>>> cogrouped = 
            pairRDD.cogroup(pairRDD2);
        System.out.println("cogroup: " + cogrouped.collect());
        
        // 9. aggregateByKey: 按键聚合（更灵活）
        JavaPairRDD<String, Integer> aggregated = pairRDD.aggregateByKey(
            0,              // 初始值
            Integer::sum,   // 分区内聚合
            Integer::sum    // 分区间聚合
        );
        System.out.println("aggregateByKey (sum): " + aggregated.collect());
        
        // 10. combineByKey: 组合值（最灵活）
        JavaPairRDD<String, Tuple2<Integer, Integer>> combined = pairRDD.combineByKey(
            v -> new Tuple2<>(v, 1),  // 创建组合器
            (acc, v) -> new Tuple2<>(acc._1 + v, acc._2 + 1),  // 分区内合并
            (acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2)  // 分区间合并
        );
        // 计算平均值
        JavaPairRDD<String, Double> averages = combined.mapValues(t -> (double) t._1 / t._2);
        System.out.println("combineByKey (average): " + averages.collect());
    }

    /**
     * 演示分区操作
     */
    public static void demonstratePartitioning(JavaSparkContext sc) {
        System.out.println("\n========== 分区操作演示 ==========");
        
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        
        // 1. 指定分区数
        JavaRDD<Integer> rdd = sc.parallelize(numbers, 4);
        System.out.println("分区数: " + rdd.getNumPartitions());
        
        // 2. repartition: 重新分区（会发生 shuffle）
        JavaRDD<Integer> repartitioned = rdd.repartition(2);
        System.out.println("重新分区后: " + repartitioned.getNumPartitions());
        
        // 3. coalesce: 减少分区（尽量避免 shuffle）
        JavaRDD<Integer> coalesced = rdd.coalesce(2);
        System.out.println("coalesce 后: " + coalesced.getNumPartitions());
        
        // 4. mapPartitions: 对每个分区进行操作
        JavaRDD<Integer> processed = rdd.mapPartitions(iter -> {
            // 在每个分区上执行的操作
            List<Integer> result = new java.util.ArrayList<>();
            while (iter.hasNext()) {
                result.add(iter.next() * 2);
            }
            return result.iterator();
        });
        System.out.println("mapPartitions (x * 2): " + processed.collect());
        
        // 5. mapPartitionsWithIndex: 带索引的分区操作
        JavaRDD<String> withIndex = rdd.mapPartitionsWithIndex((index, iter) -> {
            List<String> result = new java.util.ArrayList<>();
            while (iter.hasNext()) {
                result.add("Partition-" + index + ": " + iter.next());
            }
            return result.iterator();
        }, true);
        System.out.println("mapPartitionsWithIndex:");
        withIndex.collect().forEach(p-> System.out.println(p.toString()));
    }

    /**
     * 演示持久化和缓存
     */
    public static void demonstratePersistence(JavaSparkContext sc) {
        System.out.println("\n========== 持久化和缓存演示 ==========");
        
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = sc.parallelize(numbers);
        
        // 1. cache: 缓存到内存
        JavaRDD<Integer> cachedRDD = rdd.map(x -> {
            System.out.println("计算: " + x);
            return x * 2;
        }).cache();
        
        System.out.println("第一次计算:");
        cachedRDD.count();
        
        System.out.println("第二次计算（使用缓存）:");
        cachedRDD.count();
        
        // 2. persist: 指定存储级别
        // cachedRDD.persist(StorageLevel.MEMORY_AND_DISK());
        
        // 3. unpersist: 释放缓存
        cachedRDD.unpersist();
        
        System.out.println("缓存已释放");
    }

    /**
     * 运行所有演示
     */
    public static void runAllDemos(JavaSparkContext sc) {
        demonstrateRDDCreation(sc);
        demonstrateTransformations(sc);
        demonstrateActions(sc);
        demonstratePairRDDOperations(sc);
        demonstratePartitioning(sc);
        demonstratePersistence(sc);
    }

    /**
     * 独立运行入口
     */
    public static void main(String[] args) {
        org.apache.spark.SparkConf conf = new org.apache.spark.SparkConf()
            .setAppName("RDD Operations Demo")
            .setIfMissing("spark.master", "local[*]")
            .set("spark.driver.extraJavaOptions", 
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED")
            .set("spark.executor.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        
        System.out.println("========== RDD 操作演示开始 ==========");
        runAllDemos(sc);
        System.out.println("========== RDD 操作演示完成 ==========");
        
        sc.close();
    }
}

