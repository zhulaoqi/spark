# Spark 学习项目总览

## 🎯 项目目标

这是一个全面的 Apache Spark 学习项目，旨在帮助开发者系统地掌握 Spark 的核心概念和高级特性。

## 📂 项目文件说明

### 核心文件

| 文件 | 说明 |
|------|------|
| `pom.xml` | Maven 项目配置文件，定义了项目依赖和构建配置 |
| `docker-compose.yml` | Docker Compose 配置，用于启动 Spark 集群 |
| `README.md` | 项目主文档，包含快速开始和使用说明 |
| `.gitignore` | Git 忽略配置 |

### 文档文件

| 文件 | 说明 |
|------|------|
| `README.md` | 主要文档，快速开始指南 |
| `EXAMPLES.md` | 详细的代码示例和解释 |
| `TROUBLESHOOTING.md` | 常见问题排查指南 |
| `PROJECT_OVERVIEW.md` | 本文件，项目概览 |

### 源代码文件

| 文件 | 代码行数 | 说明 |
|------|---------|------|
| `SparkLearningMain.java` | ~200 | 主程序入口，交互式菜单 |
| `RDDOperationsDemo.java` | ~400 | RDD 操作演示，涵盖所有基础操作 |
| `DataFrameAndDatasetDemo.java` | ~350 | DataFrame/Dataset 操作演示 |
| `SparkSQLDemo.java` | ~400 | Spark SQL 查询演示 |
| `DataIODemo.java` | ~300 | 数据读写演示，多种格式支持 |
| `AdvancedFeaturesDemo.java` | ~450 | 高级特性演示，性能优化技巧 |

### 脚本文件

| 文件 | 说明 |
|------|------|
| `run.sh` | 本地运行脚本，自动编译和启动程序 |
| `run-cluster.sh` | 集群运行脚本，启动 Docker 集群并提交作业 |

### 数据文件

| 目录/文件 | 说明 |
|----------|------|
| `data/input/sample.txt` | 示例文本文件，用于词频统计演示 |
| `data/input/products.csv` | 示例 CSV 文件，包含产品数据 |
| `data/output/` | 输出目录（自动生成） |

## 📊 知识点覆盖统计

### RDD 操作（6 大类，30+ 方法）

- ✅ RDD 创建（3 种方式）
- ✅ 转换操作（10+ 方法）
- ✅ 行动操作（10+ 方法）
- ✅ Pair RDD 操作（10+ 方法）
- ✅ 分区操作（5+ 方法）
- ✅ 持久化操作

### DataFrame/Dataset 操作（50+ 方法）

- ✅ 创建方式（3 种）
- ✅ 基本操作（10+ 方法）
- ✅ 列操作（5+ 方法）
- ✅ 聚合操作（8+ 方法）
- ✅ Join 操作（5 种）
- ✅ UDF（用户自定义函数）

### Spark SQL（20+ 特性）

- ✅ 基本查询（SELECT, WHERE, ORDER BY 等）
- ✅ 聚合函数（COUNT, SUM, AVG 等）
- ✅ JOIN 操作（4 种）
- ✅ 子查询（3 种）
- ✅ 窗口函数（7+ 种）
- ✅ 复杂查询（CASE WHEN, CTE, UNION）
- ✅ 视图管理

### 数据 I/O（5+ 格式，10+ 特性）

- ✅ CSV 读写
- ✅ JSON 读写
- ✅ Parquet 读写
- ✅ ORC 读写
- ✅ Text 读写
- ✅ 分区读写
- ✅ 保存模式（4 种）
- ✅ 格式转换
- ✅ 读取选项

### 高级特性（10+ 技术）

- ✅ 广播变量
- ✅ 累加器
- ✅ 窗口函数（详细）
- ✅ 缓存和持久化
- ✅ 分区优化
- ✅ 数据倾斜处理
- ✅ 自定义分区器

## 🎓 学习路径建议

### 第 1 阶段：基础入门（1-2 天）

```
1. 阅读 README.md，了解项目结构
2. 运行 RDD 操作演示
   - 理解 RDD 的概念
   - 掌握转换和行动操作的区别
   - 熟悉 Pair RDD 操作
3. 完成练习：实现词频统计
```

### 第 2 阶段：结构化数据处理（2-3 天）

```
1. 运行 DataFrame 和 Dataset 演示
   - 理解 DataFrame 的优势
   - 掌握常用操作
   - 学习 Join 操作
2. 运行 Spark SQL 演示
   - 熟悉 SQL 语法
   - 掌握窗口函数
3. 完成练习：数据分析报表
```

### 第 3 阶段：数据 I/O（1 天）

```
1. 运行数据读写演示
   - 了解不同格式的特点
   - 学习分区策略
2. 完成练习：格式转换工具
```

### 第 4 阶段：性能优化（2-3 天）

```
1. 运行高级特性演示
   - 学习缓存策略
   - 掌握广播变量
   - 理解数据倾斜
2. 阅读 TROUBLESHOOTING.md
3. 完成练习：优化慢查询
```

### 第 5 阶段：实战项目（3-5 天）

```
1. 选择一个实际场景
   - 日志分析
   - 用户行为分析
   - 销售报表
2. 独立实现完整项目
3. 进行性能优化
```

## 📈 学习检查清单

### RDD 操作

- [ ] 能够创建 RDD
- [ ] 理解转换和行动操作的区别
- [ ] 能够使用 map、filter、reduce
- [ ] 掌握 Pair RDD 的 Join 操作
- [ ] 理解分区的作用
- [ ] 能够使用缓存优化性能

### DataFrame/Dataset

- [ ] 能够创建 DataFrame
- [ ] 掌握 select、filter、groupBy
- [ ] 能够进行 Join 操作
- [ ] 理解 Dataset 的类型安全
- [ ] 能够编写 UDF

### Spark SQL

- [ ] 能够注册临时视图
- [ ] 掌握基本 SQL 查询
- [ ] 能够使用聚合函数
- [ ] 理解窗口函数
- [ ] 能够编写复杂查询

### 数据 I/O

- [ ] 了解各种格式的特点
- [ ] 能够读写 CSV、JSON、Parquet
- [ ] 理解分区的概念
- [ ] 掌握保存模式
- [ ] 能够进行格式转换

### 高级特性

- [ ] 理解广播变量的使用场景
- [ ] 能够使用累加器
- [ ] 掌握缓存策略
- [ ] 理解数据倾斜问题
- [ ] 能够进行性能优化

### 实战能力

- [ ] 能够独立设计 Spark 作业
- [ ] 能够分析和优化性能
- [ ] 能够处理生产环境问题
- [ ] 能够进行代码审查

## 🚀 快速命令参考

### 本地运行

```bash
# 快速运行（使用脚本）
./run.sh

# 手动运行
mvn clean package
java -jar target/spark-learning-demo-1.0-SNAPSHOT.jar

# 只编译不运行
mvn clean package -DskipTests
```

### Docker 集群

```bash
# 启动集群（使用脚本）
./run-cluster.sh

# 手动启动
docker-compose up -d
mvn clean package
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --class com.spark.learning.SparkLearningMain \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark-learning-demo-1.0-SNAPSHOT.jar

# 查看 UI
open http://localhost:8080  # Master UI
open http://localhost:8082  # Worker UI
open http://localhost:4040  # Application UI

# 停止集群
docker-compose down
```

### 常用操作

```bash
# 查看日志
docker logs spark-master
docker logs spark-worker

# 进入容器
docker exec -it spark-master bash

# 清理数据
rm -rf data/output/*

# 重新编译
mvn clean compile
```

## 📚 相关资源

### 官方文档

- [Spark 官方文档](https://spark.apache.org/docs/latest/)
- [Spark Java API](https://spark.apache.org/docs/latest/api/java/index.html)
- [Spark SQL 指南](https://spark.apache.org/docs/latest/sql-programming-guide.html)

### 学习资源

- [Spark By Examples](https://sparkbyexamples.com/)
- [Databricks 学习资源](https://www.databricks.com/spark/getting-started-with-apache-spark)
- [Spark 性能调优](https://spark.apache.org/docs/latest/tuning.html)

### 社区

- [Stack Overflow](https://stackoverflow.com/questions/tagged/apache-spark)
- [Spark 用户邮件列表](https://spark.apache.org/community.html)

## 💡 使用技巧

### 1. 高效学习

- 先运行演示，观察输出
- 阅读代码和注释
- 修改参数，观察变化
- 自己实现类似功能

### 2. 调试技巧

- 使用 `show()` 查看中间结果
- 使用 `printSchema()` 查看结构
- 使用 `explain()` 查看执行计划
- 查看 Spark UI 分析性能

### 3. 性能优化

- 优先使用 DataFrame/Dataset API
- 合理使用缓存
- 注意数据倾斜
- 选择合适的文件格式
- 调整分区数

### 4. 最佳实践

- 避免使用 collect()
- 使用 take() 查看样本
- 及时释放缓存
- 使用列式存储格式
- 启用谓词下推

## 🎉 项目特色

- ✨ **全面覆盖**: 包含 Spark 所有核心知识点
- 📝 **详细注释**: 每个方法都有中文注释
- 🔧 **可运行**: 所有代码都经过测试，可直接运行
- 🐳 **容器化**: 支持 Docker 集群部署
- 📊 **实战导向**: 贴近实际应用场景
- 🚀 **持续更新**: 基于 Spark 3.5.1 最新版本

## 📞 获取帮助

遇到问题？

1. 查看 `TROUBLESHOOTING.md`
2. 查看 `EXAMPLES.md` 中的示例
3. 查阅官方文档
4. 提交 GitHub Issue

---

**祝学习顺利！** 🎓

