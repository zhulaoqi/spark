========== Spark 学习项目 - 使用说明 ==========

本项目支持多种运行方式：

1. IDE 直接运行（开发调试）
   - 运行任意 Demo 类的 main 方法
   - 需要配置 VM 参数（见 IDE 运行配置）

2. Maven 运行
   mvn clean package
   java -jar target/spark-learning-demo-1.0-SNAPSHOT.jar

3. Spark 集群提交（6个独立入口）

   # 入口1: 交互式学习（原主程序）
   spark-submit --class com.spark.learning.SparkLearningMain your.jar

   # 入口2: 只运行 RDD 演示
   spark-submit --class com.spark.learning.demo.RDDOperationsDemo your.jar

   # 入口3: 只运行 DataFrame 演示
   spark-submit --class com.spark.learning.demo.DataFrameAndDatasetDemo your.jar

   # 入口4: 只运行 SQL 演示
   spark-submit --class com.spark.learning.demo.SparkSQLDemo your.jar

   # 入口5: 只运行数据读写演示
   spark-submit --class com.spark.learning.demo.DataIODemo your.jar

   # 入口6: 只运行高级特性演示
   spark-submit --class com.spark.learning.demo.AdvancedFeaturesDemo your.jar

4. 统一调度入口（推荐生产使用）

   spark-submit --class com.spark.learning.JobDispatcher your.jar rdd
   spark-submit --class com.spark.learning.JobDispatcher your.jar dataframe
   spark-submit --class com.spark.learning.JobDispatcher your.jar sql
   spark-submit --class com.spark.learning.JobDispatcher your.jar io
   spark-submit --class com.spark.learning.JobDispatcher your.jar advanced
   spark-submit --class com.spark.learning.JobDispatcher your.jar interactive

5. Docker 集群提交示例

   # 先编译
   mvn clean package

   # 提交到集群
   docker exec -it spark-master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --class com.spark.learning.demo.RDDOperationsDemo \
     /opt/spark-apps/spark-learning-demo-1.0-SNAPSHOT.jar

========================================

关于"必须用命令吗"的回答：

- IDE运行: 不需要命令，直接右键运行 main 方法
- 本地测试: java -jar 即可
- 集群运行: 必须用 spark-submit，因为需要连接集群、分配资源
  
spark-submit 的作用：
1. 连接 Spark 集群（Master）
2. 分配资源（内存、CPU）
3. 分发 JAR 到各节点
4. 启动 Driver 和 Executor
5. 调度任务执行

不用 spark-submit 就只能本地单机运行，无法使用集群计算能力。

