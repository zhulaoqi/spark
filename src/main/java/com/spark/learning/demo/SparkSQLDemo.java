package com.spark.learning.demo;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Spark SQL 演示
 * 
 * Spark SQL 是用于处理结构化数据的 Spark 模块
 * 支持 SQL 查询、Hive 查询、以及 DataFrame/Dataset API
 */
public class SparkSQLDemo {

    /**
     * 员工类
     */
    public static class Employee implements Serializable {
        private int id;
        private String name;
        private String department;
        private double salary;
        private int age;

        public Employee() {}

        public Employee(int id, String name, String department, double salary, int age) {
            this.id = id;
            this.name = name;
            this.department = department;
            this.salary = salary;
            this.age = age;
        }

        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDepartment() { return department; }
        public void setDepartment(String department) { this.department = department; }
        public double getSalary() { return salary; }
        public void setSalary(double salary) { this.salary = salary; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
    }

    /**
     * 演示基本 SQL 查询
     */
    public static void demonstrateBasicSQL(SparkSession spark) {
        System.out.println("\n========== 基本 SQL 查询演示 ==========");
        
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "IT", 8000.0, 25),
            new Employee(2, "Bob", "HR", 6000.0, 30),
            new Employee(3, "Charlie", "IT", 12000.0, 35),
            new Employee(4, "David", "Finance", 9000.0, 28),
            new Employee(5, "Eve", "IT", 10000.0, 32),
            new Employee(6, "Frank", "HR", 5500.0, 27)
        );
        
        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);
        
        // 注册临时视图
        df.createOrReplaceTempView("employees");
        
        // 1. SELECT 查询
        System.out.println("SELECT 查询:");
        spark.sql("SELECT * FROM employees").show();
        
        // 2. WHERE 过滤
        System.out.println("WHERE 过滤 (salary > 8000):");
        spark.sql("SELECT name, salary FROM employees WHERE salary > 8000").show();
        
        // 3. ORDER BY 排序
        System.out.println("ORDER BY 排序 (按 salary 降序):");
        spark.sql("SELECT name, salary FROM employees ORDER BY salary DESC").show();
        
        // 4. GROUP BY 分组
        System.out.println("GROUP BY 分组 (按部门统计):");
        spark.sql(
            "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary " +
            "FROM employees GROUP BY department"
        ).show();
        
        // 5. HAVING 过滤分组
        System.out.println("HAVING 过滤分组 (人数 > 1):");
        spark.sql(
            "SELECT department, COUNT(*) as count " +
            "FROM employees " +
            "GROUP BY department " +
            "HAVING count > 1"
        ).show();
        
        // 6. LIMIT 限制
        System.out.println("LIMIT 限制 (前 3 条):");
        spark.sql("SELECT * FROM employees LIMIT 3").show();
        
        // 7. DISTINCT 去重
        System.out.println("DISTINCT 去重 (部门):");
        spark.sql("SELECT DISTINCT department FROM employees").show();
    }

    /**
     * 演示聚合函数
     */
    public static void demonstrateAggregateFunctions(SparkSession spark) {
        System.out.println("\n========== 聚合函数演示 ==========");
        
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "IT", 8000.0, 25),
            new Employee(2, "Bob", "HR", 6000.0, 30),
            new Employee(3, "Charlie", "IT", 12000.0, 35),
            new Employee(4, "David", "Finance", 9000.0, 28),
            new Employee(5, "Eve", "IT", 10000.0, 32)
        );
        
        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);
        df.createOrReplaceTempView("employees");
        
        System.out.println("多种聚合函数:");
        spark.sql(
            "SELECT " +
            "  COUNT(*) as total_count, " +
            "  SUM(salary) as total_salary, " +
            "  AVG(salary) as avg_salary, " +
            "  MAX(salary) as max_salary, " +
            "  MIN(salary) as min_salary, " +
            "  STDDEV(salary) as stddev_salary " +
            "FROM employees"
        ).show();
        
        System.out.println("按部门聚合:");
        spark.sql(
            "SELECT " +
            "  department, " +
            "  COUNT(*) as count, " +
            "  ROUND(AVG(salary), 2) as avg_salary, " +
            "  MAX(age) as max_age, " +
            "  MIN(age) as min_age " +
            "FROM employees " +
            "GROUP BY department"
        ).show();
    }

    /**
     * 演示 JOIN 操作
     */
    public static void demonstrateSQLJoins(SparkSession spark) {
        System.out.println("\n========== SQL JOIN 演示 ==========");
        
        // 员工表
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "IT", 8000.0, 25),
            new Employee(2, "Bob", "HR", 6000.0, 30),
            new Employee(3, "Charlie", "Finance", 12000.0, 35)
        );
        Dataset<Row> empDF = spark.createDataFrame(employees, Employee.class);
        empDF.createOrReplaceTempView("employees");
        
        // 部门表
        List<Row> departments = Arrays.asList(
            RowFactory.create("IT", "北京"),
            RowFactory.create("HR", "上海"),
            RowFactory.create("Sales", "广州")
        );
        StructType deptSchema = new StructType(new StructField[]{
            new StructField("dept_name", DataTypes.StringType, false, Metadata.empty()),
            new StructField("location", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> deptDF = spark.createDataFrame(departments, deptSchema);
        deptDF.createOrReplaceTempView("departments");
        
        System.out.println("Employees:");
        empDF.show();
        System.out.println("Departments:");
        deptDF.show();
        
        // INNER JOIN
        System.out.println("INNER JOIN:");
        spark.sql(
            "SELECT e.name, e.department, d.location " +
            "FROM employees e " +
            "INNER JOIN departments d ON e.department = d.dept_name"
        ).show();
        
        // LEFT JOIN
        System.out.println("LEFT JOIN:");
        spark.sql(
            "SELECT e.name, e.department, d.location " +
            "FROM employees e " +
            "LEFT JOIN departments d ON e.department = d.dept_name"
        ).show();
        
        // RIGHT JOIN
        System.out.println("RIGHT JOIN:");
        spark.sql(
            "SELECT e.name, e.department, d.dept_name, d.location " +
            "FROM employees e " +
            "RIGHT JOIN departments d ON e.department = d.dept_name"
        ).show();
    }

    /**
     * 演示子查询
     */
    public static void demonstrateSubqueries(SparkSession spark) {
        System.out.println("\n========== 子查询演示 ==========");
        
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "IT", 8000.0, 25),
            new Employee(2, "Bob", "HR", 6000.0, 30),
            new Employee(3, "Charlie", "IT", 12000.0, 35),
            new Employee(4, "David", "Finance", 9000.0, 28),
            new Employee(5, "Eve", "IT", 10000.0, 32)
        );
        
        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);
        df.createOrReplaceTempView("employees");
        
        // 1. WHERE 子查询
        System.out.println("WHERE 子查询 (工资高于平均工资的员工):");
        spark.sql(
            "SELECT name, salary " +
            "FROM employees " +
            "WHERE salary > (SELECT AVG(salary) FROM employees)"
        ).show();
        
        // 2. FROM 子查询
        System.out.println("FROM 子查询 (部门统计):");
        spark.sql(
            "SELECT dept_name, total_salary " +
            "FROM (SELECT department as dept_name, SUM(salary) as total_salary " +
            "      FROM employees " +
            "      GROUP BY department) AS dept_stats " +
            "WHERE total_salary > 10000"
        ).show();
        
        // 3. IN 子查询
        System.out.println("IN 子查询 (IT 和 Finance 部门的员工):");
        spark.sql(
            "SELECT name, department " +
            "FROM employees " +
            "WHERE department IN ('IT', 'Finance')"
        ).show();
    }

    /**
     * 演示窗口函数
     */
    public static void demonstrateWindowFunctions(SparkSession spark) {
        System.out.println("\n========== 窗口函数演示 ==========");
        
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "IT", 8000.0, 25),
            new Employee(2, "Bob", "HR", 6000.0, 30),
            new Employee(3, "Charlie", "IT", 12000.0, 35),
            new Employee(4, "David", "Finance", 9000.0, 28),
            new Employee(5, "Eve", "IT", 10000.0, 32),
            new Employee(6, "Frank", "HR", 5500.0, 27),
            new Employee(7, "Grace", "Finance", 11000.0, 33)
        );
        
        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);
        df.createOrReplaceTempView("employees");
        
        // 1. ROW_NUMBER: 行号
        System.out.println("ROW_NUMBER (按部门和工资排序):");
        spark.sql(
            "SELECT name, department, salary, " +
            "  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num " +
            "FROM employees"
        ).show();
        
        // 2. RANK: 排名（有并列排名，排名不连续）
        System.out.println("RANK (按工资排名):");
        spark.sql(
            "SELECT name, salary, " +
            "  RANK() OVER (ORDER BY salary DESC) as rank " +
            "FROM employees"
        ).show();
        
        // 3. DENSE_RANK: 密集排名（有并列排名，排名连续）
        System.out.println("DENSE_RANK (按工资排名):");
        spark.sql(
            "SELECT name, salary, " +
            "  DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank " +
            "FROM employees"
        ).show();
        
        // 4. NTILE: 分组
        System.out.println("NTILE (分成 3 组):");
        spark.sql(
            "SELECT name, salary, " +
            "  NTILE(3) OVER (ORDER BY salary) as quartile " +
            "FROM employees"
        ).show();
        
        // 5. LAG: 前一行的值
        System.out.println("LAG (前一个员工的工资):");
        spark.sql(
            "SELECT name, salary, " +
            "  LAG(salary, 1) OVER (ORDER BY salary) as prev_salary " +
            "FROM employees"
        ).show();
        
        // 6. LEAD: 后一行的值
        System.out.println("LEAD (后一个员工的工资):");
        spark.sql(
            "SELECT name, salary, " +
            "  LEAD(salary, 1) OVER (ORDER BY salary) as next_salary " +
            "FROM employees"
        ).show();
        
        // 7. 聚合窗口函数
        System.out.println("聚合窗口函数 (部门平均工资和累计工资):");
        spark.sql(
            "SELECT name, department, salary, " +
            "  ROUND(AVG(salary) OVER (PARTITION BY department), 2) as dept_avg_salary, " +
            "  SUM(salary) OVER (PARTITION BY department ORDER BY salary " +
            "                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_salary " +
            "FROM employees"
        ).show();
    }

    /**
     * 演示复杂查询
     */
    public static void demonstrateComplexQueries(SparkSession spark) {
        System.out.println("\n========== 复杂查询演示 ==========");
        
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "IT", 8000.0, 25),
            new Employee(2, "Bob", "HR", 6000.0, 30),
            new Employee(3, "Charlie", "IT", 12000.0, 35),
            new Employee(4, "David", "Finance", 9000.0, 28),
            new Employee(5, "Eve", "IT", 10000.0, 32),
            new Employee(6, "Frank", "HR", 5500.0, 27),
            new Employee(7, "Grace", "Finance", 11000.0, 33)
        );
        
        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);
        df.createOrReplaceTempView("employees");
        
        // 1. CASE WHEN 条件表达式
        System.out.println("CASE WHEN (工资等级):");
        spark.sql(
            "SELECT name, salary, " +
            "  CASE " +
            "    WHEN salary < 7000 THEN '低' " +
            "    WHEN salary < 10000 THEN '中' " +
            "    ELSE '高' " +
            "  END as salary_level " +
            "FROM employees"
        ).show();
        
        // 2. WITH (CTE - 公共表表达式)
        System.out.println("WITH (CTE):");
        spark.sql(
            "WITH dept_stats AS ( " +
            "  SELECT department, AVG(salary) as avg_salary " +
            "  FROM employees " +
            "  GROUP BY department " +
            ") " +
            "SELECT e.name, e.department, e.salary, ROUND(d.avg_salary, 2) as dept_avg " +
            "FROM employees e " +
            "JOIN dept_stats d ON e.department = d.department " +
            "WHERE e.salary > d.avg_salary"
        ).show();
        
        // 3. UNION 联合查询
        System.out.println("UNION (高薪和高龄员工):");
        spark.sql(
            "SELECT name, 'High Salary' as type FROM employees WHERE salary > 10000 " +
            "UNION " +
            "SELECT name, 'Senior' as type FROM employees WHERE age > 32"
        ).show();
    }

    /**
     * 演示全局临时视图
     */
    public static void demonstrateGlobalTempView(SparkSession spark) {
        System.out.println("\n========== 全局临时视图演示 ==========");
        
        List<Employee> employees = Arrays.asList(
            new Employee(1, "Alice", "IT", 8000.0, 25),
            new Employee(2, "Bob", "HR", 6000.0, 30)
        );
        
        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);
        
        try {
            // 创建全局临时视图
            df.createGlobalTempView("global_employees");
            
            // 使用全局临时视图（需要加 global_temp 前缀）
            System.out.println("查询全局临时视图:");
            spark.sql("SELECT * FROM global_temp.global_employees").show();
            
            // 全局临时视图可以跨 SparkSession 使用
            System.out.println("全局临时视图已创建，可以在其他 SparkSession 中访问");
        } catch (Exception e) {
            System.out.println("全局临时视图可能已存在，跳过创建");
        }
    }

    /**
     * 运行所有演示
     */
    public static void runAllDemos(SparkSession spark) {
        demonstrateBasicSQL(spark);
        demonstrateAggregateFunctions(spark);
        demonstrateSQLJoins(spark);
        demonstrateSubqueries(spark);
        demonstrateWindowFunctions(spark);
        demonstrateComplexQueries(spark);
        demonstrateGlobalTempView(spark);
    }

    /**
     * 独立运行入口
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Spark SQL Demo")
            .config("spark.master", "local[*]")
            .config("spark.driver.extraJavaOptions", 
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED")
            .config("spark.executor.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED")
            .getOrCreate();
        
        spark.sparkContext().setLogLevel("WARN");
        
        System.out.println("========== Spark SQL 演示开始 ==========");
        runAllDemos(spark);
        System.out.println("========== Spark SQL 演示完成 ==========");
        
        spark.stop();
    }
}

