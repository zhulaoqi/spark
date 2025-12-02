package com.spark.learning.demo;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * DataFrame 和 Dataset 操作演示
 * 
 * DataFrame: 类似于关系数据库中的表，是一种以列的形式组织的分布式数据集
 * Dataset: 是 DataFrame 的类型安全版本，提供编译时类型检查
 */
public class DataFrameAndDatasetDemo {

    /**
     * 用户实体类
     */
    public static class User implements Serializable {
        private String name;
        private int age;
        private String city;
        private double salary;

        public User() {}

        public User(String name, int age, String city, double salary) {
            this.name = name;
            this.age = age;
            this.city = city;
            this.salary = salary;
        }

        // Getters and Setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }
        public double getSalary() { return salary; }
        public void setSalary(double salary) { this.salary = salary; }

        @Override
        public String toString() {
            return "User{name='" + name + "', age=" + age + 
                   ", city='" + city + "', salary=" + salary + "}";
        }
    }

    /**
     * 演示 DataFrame 的创建
     */
    public static void demonstrateDataFrameCreation(SparkSession spark) {
        System.out.println("\n========== DataFrame 创建演示 ==========");
        
        // 方式1: 从集合创建
        List<Row> data = Arrays.asList(
            RowFactory.create("Alice", 25, "Beijing", 8000.0),
            RowFactory.create("Bob", 30, "Shanghai", 12000.0),
            RowFactory.create("Charlie", 35, "Guangzhou", 15000.0)
        );
        
        StructType schema = new StructType(new StructField[]{
            new StructField("name", DataTypes.StringType, false, Metadata.empty()),
            new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("city", DataTypes.StringType, false, Metadata.empty()),
            new StructField("salary", DataTypes.DoubleType, false, Metadata.empty())
        });
        
        Dataset<Row> df1 = spark.createDataFrame(data, schema);
        System.out.println("从集合创建的 DataFrame:");
        df1.show();
        
        // 方式2: 从 Bean 类创建
        List<User> users = Arrays.asList(
            new User("David", 28, "Shenzhen", 10000.0),
            new User("Eve", 32, "Beijing", 13000.0),
            new User("Frank", 27, "Shanghai", 9500.0)
        );
        Dataset<Row> df2 = spark.createDataFrame(users, User.class);
        System.out.println("从 Bean 类创建的 DataFrame:");
        df2.show();
        
        // 方式3: 从 RDD 创建
        // Dataset<Row> df3 = spark.createDataFrame(rdd, User.class);
    }

    /**
     * 演示 Dataset 的创建和使用
     */
    public static void demonstrateDataset(SparkSession spark) {
        System.out.println("\n========== Dataset 操作演示 ==========");
        
        List<User> users = Arrays.asList(
            new User("Alice", 25, "Beijing", 8000.0),
            new User("Bob", 30, "Shanghai", 12000.0),
            new User("Charlie", 35, "Guangzhou", 15000.0),
            new User("David", 28, "Shenzhen", 10000.0),
            new User("Eve", 32, "Beijing", 13000.0)
        );
        
        // 创建 Dataset
        Dataset<User> ds = spark.createDataset(users, Encoders.bean(User.class));
        System.out.println("Dataset:");
        ds.show();
        
        // Dataset 操作（类型安全）
        Dataset<String> names = ds.map(
            (org.apache.spark.api.java.function.MapFunction<User, String>) user -> user.getName(), 
            Encoders.STRING()
        );
        System.out.println("所有用户名:");
        names.show();
        
        // 过滤
        Dataset<User> filtered = ds.filter(
            (org.apache.spark.api.java.function.FilterFunction<User>) user -> user.getAge() > 28
        );
        System.out.println("年龄大于 28 的用户:");
        filtered.show();
        
        // 转换为 DataFrame
        Dataset<Row> df = ds.toDF();
        System.out.println("转换为 DataFrame:");
        df.show();
    }

    /**
     * 演示 DataFrame 基本操作
     */
    public static void demonstrateBasicOperations(SparkSession spark) {
        System.out.println("\n========== DataFrame 基本操作演示 ==========");
        
        List<User> users = Arrays.asList(
            new User("Alice", 25, "Beijing", 8000.0),
            new User("Bob", 30, "Shanghai", 12000.0),
            new User("Charlie", 35, "Guangzhou", 15000.0),
            new User("David", 28, "Shenzhen", 10000.0),
            new User("Eve", 32, "Beijing", 13000.0),
            new User("Frank", 27, "Shanghai", 9500.0)
        );
        Dataset<Row> df = spark.createDataFrame(users, User.class);
        
        // 1. show: 显示数据
        System.out.println("show (默认显示 20 行):");
        df.show();
        
        // 2. printSchema: 打印结构
        System.out.println("printSchema:");
        df.printSchema();
        
        // 3. select: 选择列
        System.out.println("select (name, age):");
        df.select("name", "age").show();
        
        // 4. selectExpr: 使用表达式选择
        System.out.println("selectExpr (name, age + 10):");
        df.selectExpr("name", "age + 10 as age_plus_10").show();
        
        // 5. filter / where: 过滤
        System.out.println("filter (age > 28):");
        df.filter("age > 28").show();
        df.where(col("age").gt(28)).show();
        
        // 6. distinct: 去重
        System.out.println("distinct cities:");
        df.select("city").distinct().show();
        
        // 7. orderBy / sort: 排序
        System.out.println("orderBy (salary desc):");
        df.orderBy(col("salary").desc()).show();
        
        // 8. limit: 限制行数
        System.out.println("limit (3):");
        df.limit(3).show();
        
        // 9. withColumn: 添加或修改列
        System.out.println("withColumn (salary_k):");
        df.withColumn("salary_k", col("salary").divide(1000)).show();
        
        // 10. withColumnRenamed: 重命名列
        System.out.println("withColumnRenamed (name -> user_name):");
        df.withColumnRenamed("name", "user_name").show();
        
        // 11. drop: 删除列
        System.out.println("drop (city):");
        df.drop("city").show();
        
        // 12. dropDuplicates: 去除重复行
        System.out.println("dropDuplicates (by city):");
        df.dropDuplicates("city").show();
    }

    /**
     * 演示聚合操作
     */
    public static void demonstrateAggregations(SparkSession spark) {
        System.out.println("\n========== 聚合操作演示 ==========");
        
        List<User> users = Arrays.asList(
            new User("Alice", 25, "Beijing", 8000.0),
            new User("Bob", 30, "Shanghai", 12000.0),
            new User("Charlie", 35, "Beijing", 15000.0),
            new User("David", 28, "Shanghai", 10000.0),
            new User("Eve", 32, "Beijing", 13000.0),
            new User("Frank", 27, "Shanghai", 9500.0)
        );
        Dataset<Row> df = spark.createDataFrame(users, User.class);
        
        // 1. count: 计数
        System.out.println("count: " + df.count());
        
        // 2. groupBy: 分组
        System.out.println("groupBy (city) + count:");
        df.groupBy("city").count().show();
        
        // 3. 多种聚合函数
        System.out.println("groupBy (city) + 多种聚合:");
        df.groupBy("city")
          .agg(
              count("*").alias("count"),
              avg("age").alias("avg_age"),
              max("salary").alias("max_salary"),
              min("salary").alias("min_salary"),
              sum("salary").alias("total_salary")
          ).show();
        
        // 4. agg: 直接聚合
        System.out.println("agg (全局聚合):");
        df.agg(
            avg("age").alias("avg_age"),
            max("salary").alias("max_salary")
        ).show();
        
        // 5. describe: 统计描述
        System.out.println("describe:");
        df.describe("age", "salary").show();
    }

    /**
     * 演示 Join 操作
     */
    public static void demonstrateJoins(SparkSession spark) {
        System.out.println("\n========== Join 操作演示 ==========");
        
        // 创建两个 DataFrame
        List<Row> userData = Arrays.asList(
            RowFactory.create(1, "Alice", "Beijing"),
            RowFactory.create(2, "Bob", "Shanghai"),
            RowFactory.create(3, "Charlie", "Guangzhou")
        );
        StructType userSchema = new StructType(new StructField[]{
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("name", DataTypes.StringType, false, Metadata.empty()),
            new StructField("city", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> users = spark.createDataFrame(userData, userSchema);
        
        List<Row> orderData = Arrays.asList(
            RowFactory.create(101, 1, 100.0),
            RowFactory.create(102, 2, 200.0),
            RowFactory.create(103, 1, 150.0),
            RowFactory.create(104, 4, 300.0)  // user_id = 4 不存在
        );
        StructType orderSchema = new StructType(new StructField[]{
            new StructField("order_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("amount", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> orders = spark.createDataFrame(orderData, orderSchema);
        
        System.out.println("Users:");
        users.show();
        System.out.println("Orders:");
        orders.show();
        
        // 1. inner join (内连接)
        System.out.println("Inner Join:");
        users.join(orders, users.col("id").equalTo(orders.col("user_id")), "inner")
             .show();
        
        // 2. left outer join (左外连接)
        System.out.println("Left Outer Join:");
        users.join(orders, users.col("id").equalTo(orders.col("user_id")), "left")
             .show();
        
        // 3. right outer join (右外连接)
        System.out.println("Right Outer Join:");
        users.join(orders, users.col("id").equalTo(orders.col("user_id")), "right")
             .show();
        
        // 4. full outer join (全外连接)
        System.out.println("Full Outer Join:");
        users.join(orders, users.col("id").equalTo(orders.col("user_id")), "full")
             .show();
        
        // 5. cross join (交叉连接)
        System.out.println("Cross Join:");
        users.crossJoin(orders).show();
    }

    /**
     * 演示 UDF (用户自定义函数)
     */
    public static void demonstrateUDF(SparkSession spark) {
        System.out.println("\n========== UDF 演示 ==========");
        
        List<User> users = Arrays.asList(
            new User("Alice", 25, "Beijing", 8000.0),
            new User("Bob", 30, "Shanghai", 12000.0),
            new User("Charlie", 35, "Guangzhou", 15000.0)
        );
        Dataset<Row> df = spark.createDataFrame(users, User.class);
        
        // 注册 UDF
        spark.udf().register("ageCategory", (Integer age) -> {
            if (age < 30) return "青年";
            else if (age < 40) return "中年";
            else return "老年";
        }, DataTypes.StringType);
        
        // 使用 UDF
        df.createOrReplaceTempView("users");
        Dataset<Row> result = spark.sql(
            "SELECT name, age, ageCategory(age) as category FROM users"
        );
        
        System.out.println("使用 UDF 分类年龄:");
        result.show();
    }

    /**
     * 运行所有演示
     */
    public static void runAllDemos(SparkSession spark) {
        demonstrateDataFrameCreation(spark);
        demonstrateDataset(spark);
        demonstrateBasicOperations(spark);
        demonstrateAggregations(spark);
        demonstrateJoins(spark);
        demonstrateUDF(spark);
    }
}

