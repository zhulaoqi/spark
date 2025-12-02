# Spark 学习演示项目

## 项目简介

这是一个全面的 Apache Spark 学习项目，使用 Java 21 开发，涵盖了 Spark 的所有核心概念和高级特性。项目包含了丰富的示例代码和详细的注释，适合初学者学习和进阶开发者参考。

## 技术栈

- **Java**: 21
- **Spark**: 3.5.1
- **Scala**: 2.12
- **Hadoop**: 3.3.6
- **Maven**: 项目管理和构建工具
- **Docker**: 容器化部署

## 项目结构

```
spark/
├── pom.xml                                    # Maven 项目配置
├── docker-compose.yml                         # Docker Compose 配置
├── README.md                                  # 项目说明文档
├── .gitignore                                 # Git 忽略文件
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── spark/
│       │           └── learning/
│       │               ├── SparkLearningMain.java              # 主程序入口
│       │               └── demo/
│       │                   ├── RDDOperationsDemo.java          # RDD 操作演示
│       │                   ├── DataFrameAndDatasetDemo.java    # DataFrame/Dataset 演示
│       │                   ├── SparkSQLDemo.java               # Spark SQL 演示
│       │                   ├── DataIODemo.java                 # 数据读写演示
│       │                   └── AdvancedFeaturesDemo.java       # 高级特性演示
│       └── resources/
│           └── log4j.properties               # 日志配置
└── data/                                      # 数据文件目录
    ├── input/                                 # 输入数据
    └── output/                                # 输出数据
```

## 核心知识点

### 1. RDD 操作 (RDDOperationsDemo)

- **RDD 创建**: 从集合、文件、其他 RDD 创建
- **转换操作**: map, filter, flatMap, distinct, union, intersection, subtract
- **行动操作**: collect, count, first, take, reduce, fold, aggregate, foreach
- **Pair RDD 操作**: reduceByKey, groupByKey, mapValues, join, cogroup, sortByKey
- **分区操作**: repartition, coalesce, mapPartitions, mapPartitionsWithIndex
- **持久化**: cache, persist, unpersist

### 2. DataFrame 和 Dataset (DataFrameAndDatasetDemo)

- **创建方式**: 从集合、Bean 类、RDD 创建
- **基本操作**: select, filter, where, distinct, orderBy, limit
- **列操作**: withColumn, withColumnRenamed, drop, dropDuplicates
- **聚合操作**: groupBy, agg, count, avg, max, min, sum
- **Join 操作**: inner, left, right, full, cross join
- **UDF**: 用户自定义函数

### 3. Spark SQL (SparkSQLDemo)

- **基本查询**: SELECT, WHERE, ORDER BY, LIMIT, DISTINCT
- **聚合函数**: COUNT, SUM, AVG, MAX, MIN, STDDEV
- **JOIN 操作**: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN
- **子查询**: WHERE 子查询, FROM 子查询, IN 子查询
- **窗口函数**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD
- **复杂查询**: CASE WHEN, WITH (CTE), UNION
- **视图管理**: 临时视图、全局临时视图

### 4. 数据读写 (DataIODemo)

- **文件格式**:
  - CSV: 文本格式，易读但性能较低
  - JSON: 半结构化数据，灵活但占用空间大
  - Parquet: 列式存储，高效压缩，Spark 推荐格式
  - ORC: 优化的行列式文件格式
  - Text: 纯文本文件
- **保存模式**: Overwrite, Append, ErrorIfExists, Ignore
- **分区写入**: partitionBy
- **读取选项**: header, inferSchema, sep, dateFormat 等
- **格式转换**: CSV ↔ Parquet ↔ JSON

### 5. 高级特性 (AdvancedFeaturesDemo)

- **广播变量**: 高效分发大型只读数据到各个节点
- **累加器**: 实现分布式计数器和求和
- **窗口函数**: 
  - 排名函数: rank, dense_rank, row_number
  - 分析函数: lag, lead
  - 聚合窗口: 移动平均、累计求和
- **缓存和持久化**: 优化重复计算
- **分区优化**: repartition, coalesce, 自定义分区器
- **数据倾斜处理**: 加盐 (Salting)、两阶段聚合

## 快速开始

### 方式一：本地运行（推荐使用脚本）

#### 前置要求

- JDK 21
- Maven 3.6+
- Git

#### 步骤

1. **克隆项目**
   ```bash
   git clone <repository-url>
   cd spark
   ```

2. **使用脚本运行（推荐）**
   ```bash
   # 赋予执行权限（首次）
   chmod +x run.sh
   
   # 运行程序
   ./run.sh
   ```

3. **手动运行**
   
   如果不使用脚本，需要添加 Java 模块参数：
   
   ```bash
   # 编译项目
   mvn clean package
   
   # 运行程序（Java 17+）
   java --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
        --add-opens java.base/java.lang=ALL-UNNAMED \
        --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
        --add-opens java.base/java.io=ALL-UNNAMED \
        --add-opens java.base/java.util=ALL-UNNAMED \
        -jar target/spark-learning-demo-1.0-SNAPSHOT.jar
   ```

4. **选择演示**
   程序会显示交互式菜单，选择要运行的演示：
   - 1: RDD 操作演示
   - 2: DataFrame 和 Dataset 操作演示
   - 3: Spark SQL 演示
   - 4: 数据读写演示
   - 5: 高级特性演示
   - 6: 运行所有演示
   - 0: 退出

### 方式二：Docker 集群运行

#### 前置要求

- Docker
- Docker Compose

#### 步骤

1. **启动 Spark 集群**
   ```bash
   docker-compose up -d
   ```

2. **查看集群状态**
   - Spark Master UI: http://localhost:8080
   - Spark Worker UI: http://localhost:8082
   - Application UI: http://localhost:4040 (运行作业时)

3. **编译项目**
   ```bash
   mvn clean package
   ```

4. **提交作业到集群**
   ```bash
   docker exec -it spark-master /opt/spark/bin/spark-submit \
     --class com.spark.learning.SparkLearningMain \
     --master spark://spark-master:7077 \
     /opt/spark-apps/spark-learning-demo-1.0-SNAPSHOT.jar
   ```

5. **停止集群**
   ```bash
   docker-compose down
   ```

## 使用说明

### 交互式菜单

程序启动后会显示交互式菜单，您可以：

1. 选择单个演示模块运行，查看特定功能
2. 选择运行所有演示，完整体验所有功能
3. 每个演示完成后会暂停，按 Enter 继续

### 查看输出

- **控制台输出**: 所有演示结果都会直接打印到控制台
- **数据文件**: 数据读写演示会在 `data/output/` 目录下生成文件
- **日志文件**: 详细日志保存在 Spark 日志目录

### 修改配置

在 `SparkLearningMain.java` 中可以修改 Spark 配置：

```java
SparkConf conf = new SparkConf()
    .setAppName("Spark Learning Demo")
    .setMaster("local[*]")  // 修改为集群地址
    .set("spark.sql.shuffle.partitions", "4")
    .set("spark.default.parallelism", "4");
```

## Docker 集群配置

### 服务说明

- **spark-master**: Spark 主节点
  - 端口 8080: Web UI
  - 端口 7077: 集群通信
  - 端口 4040: 应用 UI

- **spark-worker**: Spark 工作节点
  - 端口 8082: Web UI
  - 配置: 2 核 CPU, 2GB 内存

### 扩展 Worker 节点

在 `docker-compose.yml` 中添加更多 worker:

```yaml
spark-worker-2:
  image: apache/spark:3.5.1
  container_name: spark-worker-2
  command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_MEMORY=2G
  ports:
    - "8083:8082"
  depends_on:
    spark-master:
      condition: service_healthy
```

## 学习路径

### 初学者

1. 先运行 **RDD 操作演示**，理解 Spark 的基础数据结构
2. 学习 **DataFrame 和 Dataset 演示**，掌握结构化数据处理
3. 熟悉 **Spark SQL 演示**，学习 SQL 查询方式

### 进阶学习

4. 研究 **数据读写演示**，了解不同数据格式的特点
5. 深入 **高级特性演示**，掌握性能优化技巧

### 实践建议

- 修改示例代码，尝试不同的参数
- 使用自己的数据集进行实验
- 阅读代码注释，理解每个操作的含义
- 对比不同方法的性能差异

## 性能优化建议

### 1. 选择合适的算子

- 优先使用 `reduceByKey` 而不是 `groupByKey`
- 使用 `mapPartitions` 代替 `map` 处理批量数据
- 合理使用 `filter` 减少数据量

### 2. 缓存策略

```java
// 对重复使用的 RDD/DataFrame 进行缓存
df.cache();
// 或指定存储级别
df.persist(StorageLevel.MEMORY_AND_DISK());
```

### 3. 分区优化

```java
// 增加分区提高并行度
df.repartition(100);
// 减少分区降低 shuffle 开销
df.coalesce(10);
```

### 4. 广播变量

```java
// 对大型查找表使用广播变量
Broadcast<Map<String, String>> broadcast = sc.broadcast(lookupMap);
```

### 5. 数据倾斜处理

- 使用加盐 (Salting) 技术
- 两阶段聚合
- 合理设计分区键

## 常见问题

### Q1: 内存不足错误

**A**: 调整 Spark 内存配置：
```java
.set("spark.executor.memory", "4g")
.set("spark.driver.memory", "2g")
```

### Q2: 分区数过多或过少

**A**: 调整分区配置：
```java
.set("spark.sql.shuffle.partitions", "200")  // 默认 200
.set("spark.default.parallelism", "100")
```

### Q3: 数据倾斜导致性能问题

**A**: 参考 `AdvancedFeaturesDemo` 中的数据倾斜处理方法

### Q4: Docker 容器无法启动

**A**: 
- 检查端口是否被占用
- 确保 Docker 有足够的资源分配
- 查看容器日志: `docker logs spark-master`

## 相关资源

- [Apache Spark 官方文档](https://spark.apache.org/docs/latest/)
- [Spark Java API 文档](https://spark.apache.org/docs/latest/api/java/index.html)
- [Spark SQL 指南](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [性能调优指南](https://spark.apache.org/docs/latest/tuning.html)

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License

## 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 GitHub Issue
- 发送邮件至: [1647110340@qq.com]

---

**祝您学习愉快！Happy Sparking! 🚀**

