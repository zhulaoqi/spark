package com.spark.learning.demo;

import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 数据读写演示
 * 
 * 演示 Spark 如何读取和写入不同格式的数据
 */
public class DataIODemo {

    /**
     * 产品类
     */
    public static class Product implements Serializable {
        private int id;
        private String name;
        private String category;
        private double price;

        public Product() {}

        public Product(int id, String name, String category, double price) {
            this.id = id;
            this.name = name;
            this.category = category;
            this.price = price;
        }

        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        public double getPrice() { return price; }
        public void setPrice(double price) { this.price = price; }
    }

    /**
     * 演示 CSV 文件读写
     */
    public static void demonstrateCSVIO(SparkSession spark) {
        System.out.println("\n========== CSV 文件读写演示 ==========");
        
        List<Product> products = Arrays.asList(
            new Product(1, "iPhone", "Electronics", 999.99),
            new Product(2, "MacBook", "Electronics", 1999.99),
            new Product(3, "Desk", "Furniture", 299.99),
            new Product(4, "Chair", "Furniture", 199.99)
        );
        
        Dataset<Row> df = spark.createDataFrame(products, Product.class);
        
        // 写入 CSV
        String csvPath = "data/output/products.csv";
        df.write()
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv(csvPath);
        System.out.println("CSV 文件已写入: " + csvPath);
        
        // 读取 CSV
        Dataset<Row> readDF = spark.read()
                                   .option("header", "true")
                                   .option("inferSchema", "true")
                                   .csv(csvPath);
        System.out.println("从 CSV 读取的数据:");
        readDF.show();
        readDF.printSchema();
    }

    /**
     * 演示 JSON 文件读写
     */
    public static void demonstrateJSONIO(SparkSession spark) {
        System.out.println("\n========== JSON 文件读写演示 ==========");
        
        List<Product> products = Arrays.asList(
            new Product(1, "iPhone", "Electronics", 999.99),
            new Product(2, "MacBook", "Electronics", 1999.99),
            new Product(3, "Desk", "Furniture", 299.99)
        );
        
        Dataset<Row> df = spark.createDataFrame(products, Product.class);
        
        // 写入 JSON
        String jsonPath = "data/output/products.json";
        df.write()
          .mode(SaveMode.Overwrite)
          .json(jsonPath);
        System.out.println("JSON 文件已写入: " + jsonPath);
        
        // 读取 JSON
        Dataset<Row> readDF = spark.read().json(jsonPath);
        System.out.println("从 JSON 读取的数据:");
        readDF.show();
        readDF.printSchema();
    }

    /**
     * 演示 Parquet 文件读写
     */
    public static void demonstrateParquetIO(SparkSession spark) {
        System.out.println("\n========== Parquet 文件读写演示 ==========");
        
        List<Product> products = Arrays.asList(
            new Product(1, "iPhone", "Electronics", 999.99),
            new Product(2, "MacBook", "Electronics", 1999.99),
            new Product(3, "Desk", "Furniture", 299.99)
        );
        
        Dataset<Row> df = spark.createDataFrame(products, Product.class);
        
        // 写入 Parquet（Spark 默认格式，列式存储，高效压缩）
        String parquetPath = "data/output/products.parquet";
        df.write()
          .mode(SaveMode.Overwrite)
          .parquet(parquetPath);
        System.out.println("Parquet 文件已写入: " + parquetPath);
        
        // 读取 Parquet
        Dataset<Row> readDF = spark.read().parquet(parquetPath);
        System.out.println("从 Parquet 读取的数据:");
        readDF.show();
        readDF.printSchema();
    }

    /**
     * 演示 ORC 文件读写
     */
    public static void demonstrateORCIO(SparkSession spark) {
        System.out.println("\n========== ORC 文件读写演示 ==========");
        
        List<Product> products = Arrays.asList(
            new Product(1, "iPhone", "Electronics", 999.99),
            new Product(2, "MacBook", "Electronics", 1999.99)
        );
        
        Dataset<Row> df = spark.createDataFrame(products, Product.class);
        
        // 写入 ORC（优化的行列式文件格式）
        String orcPath = "data/output/products.orc";
        df.write()
          .mode(SaveMode.Overwrite)
          .orc(orcPath);
        System.out.println("ORC 文件已写入: " + orcPath);
        
        // 读取 ORC
        Dataset<Row> readDF = spark.read().orc(orcPath);
        System.out.println("从 ORC 读取的数据:");
        readDF.show();
    }

    /**
     * 演示文本文件读写
     */
    public static void demonstrateTextIO(SparkSession spark) {
        System.out.println("\n========== 文本文件读写演示 ==========");
        
        // 创建简单的文本数据
        List<String> lines = Arrays.asList(
            "This is line 1",
            "This is line 2",
            "This is line 3"
        );
        
        Dataset<Row> df = spark.createDataset(lines, Encoders.STRING()).toDF("value");
        
        // 写入文本文件
        String textPath = "data/output/lines.txt";
        df.write()
          .mode(SaveMode.Overwrite)
          .text(textPath);
        System.out.println("文本文件已写入: " + textPath);
        
        // 读取文本文件
        Dataset<Row> readDF = spark.read().text(textPath);
        System.out.println("从文本文件读取的数据:");
        readDF.show();
    }

    /**
     * 演示分区写入
     */
    public static void demonstratePartitionedWrite(SparkSession spark) {
        System.out.println("\n========== 分区写入演示 ==========");
        
        List<Product> products = Arrays.asList(
            new Product(1, "iPhone", "Electronics", 999.99),
            new Product(2, "MacBook", "Electronics", 1999.99),
            new Product(3, "Desk", "Furniture", 299.99),
            new Product(4, "Chair", "Furniture", 199.99),
            new Product(5, "Lamp", "Furniture", 49.99)
        );
        
        Dataset<Row> df = spark.createDataFrame(products, Product.class);
        
        // 按 category 分区写入
        String partitionedPath = "data/output/products_partitioned";
        df.write()
          .mode(SaveMode.Overwrite)
          .partitionBy("category")
          .parquet(partitionedPath);
        System.out.println("分区数据已写入: " + partitionedPath);
        
        // 读取分区数据
        Dataset<Row> readDF = spark.read().parquet(partitionedPath);
        System.out.println("从分区数据读取:");
        readDF.show();
        
        // 读取特定分区
        Dataset<Row> electronicsDF = spark.read()
                                          .parquet(partitionedPath + "/category=Electronics");
        System.out.println("只读取 Electronics 分区:");
        electronicsDF.show();
    }

    /**
     * 演示不同的保存模式
     */
    public static void demonstrateSaveModes(SparkSession spark) {
        System.out.println("\n========== 保存模式演示 ==========");
        
        List<Product> products = Arrays.asList(
            new Product(1, "iPhone", "Electronics", 999.99)
        );
        
        Dataset<Row> df = spark.createDataFrame(products, Product.class);
        
        String basePath = "data/output/save_modes";
        
        // SaveMode.Overwrite: 覆盖
        System.out.println("SaveMode.Overwrite - 覆盖已存在的数据");
        df.write().mode(SaveMode.Overwrite).parquet(basePath + "/overwrite");
        
        // SaveMode.Append: 追加
        System.out.println("SaveMode.Append - 追加到已存在的数据");
        df.write().mode(SaveMode.Append).parquet(basePath + "/append");
        
        // SaveMode.ErrorIfExists: 如果存在则报错（默认）
        System.out.println("SaveMode.ErrorIfExists - 如果存在则报错");
        try {
            df.write().mode(SaveMode.ErrorIfExists).parquet(basePath + "/error");
        } catch (Exception e) {
            System.out.println("捕获到异常: " + e.getMessage());
        }
        
        // SaveMode.Ignore: 如果存在则忽略
        System.out.println("SaveMode.Ignore - 如果存在则忽略");
        df.write().mode(SaveMode.Ignore).parquet(basePath + "/ignore");
        
        System.out.println("保存模式演示完成");
    }

    /**
     * 演示数据格式转换
     */
    public static void demonstrateFormatConversion(SparkSession spark) {
        System.out.println("\n========== 数据格式转换演示 ==========");
        
        List<Product> products = Arrays.asList(
            new Product(1, "iPhone", "Electronics", 999.99),
            new Product(2, "MacBook", "Electronics", 1999.99)
        );
        
        Dataset<Row> df = spark.createDataFrame(products, Product.class);
        
        // CSV -> Parquet
        String csvPath = "data/output/conversion/products.csv";
        String parquetPath = "data/output/conversion/products.parquet";
        
        df.write().mode(SaveMode.Overwrite).option("header", "true").csv(csvPath);
        System.out.println("写入 CSV: " + csvPath);
        
        Dataset<Row> csvDF = spark.read().option("header", "true").option("inferSchema", "true").csv(csvPath);
        csvDF.write().mode(SaveMode.Overwrite).parquet(parquetPath);
        System.out.println("转换为 Parquet: " + parquetPath);
        
        // Parquet -> JSON
        String jsonPath = "data/output/conversion/products.json";
        Dataset<Row> parquetDF = spark.read().parquet(parquetPath);
        parquetDF.write().mode(SaveMode.Overwrite).json(jsonPath);
        System.out.println("转换为 JSON: " + jsonPath);
        
        System.out.println("格式转换完成");
    }

    /**
     * 演示读取选项
     */
    public static void demonstrateReadOptions(SparkSession spark) {
        System.out.println("\n========== 读取选项演示 ==========");
        
        List<Product> products = Arrays.asList(
            new Product(1, "iPhone", "Electronics", 999.99),
            new Product(2, "MacBook", "Electronics", 1999.99),
            new Product(3, "Desk", "Furniture", 299.99)
        );
        
        Dataset<Row> df = spark.createDataFrame(products, Product.class);
        
        // 写入 CSV 用于演示
        String csvPath = "data/output/options/products.csv";
        df.write()
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv(csvPath);
        
        // 读取时指定各种选项
        Dataset<Row> readDF = spark.read()
            .option("header", "true")           // 第一行是标题
            .option("inferSchema", "true")      // 自动推断数据类型
            .option("sep", ",")                 // 分隔符
            .option("quote", "\"")              // 引号字符
            .option("escape", "\\")             // 转义字符
            .option("nullValue", "NULL")        // NULL 值表示
            .option("dateFormat", "yyyy-MM-dd") // 日期格式
            .csv(csvPath);
        
        System.out.println("使用选项读取的数据:");
        readDF.show();
        readDF.printSchema();
    }

    /**
     * 运行所有演示
     */
    public static void runAllDemos(SparkSession spark) {
        demonstrateCSVIO(spark);
        demonstrateJSONIO(spark);
        demonstrateParquetIO(spark);
        demonstrateORCIO(spark);
        demonstrateTextIO(spark);
        demonstratePartitionedWrite(spark);
        demonstrateSaveModes(spark);
        demonstrateFormatConversion(spark);
        demonstrateReadOptions(spark);
    }
}

