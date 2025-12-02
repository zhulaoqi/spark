package com.spark.learning;

import com.spark.learning.demo.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

/**
 * Spark 学习演示主程序
 * 
 * 这是一个全面的 Spark 学习项目，涵盖了 Spark 的核心概念和高级特性
 */
public class SparkLearningMain {

    private static SparkSession spark;
    private static JavaSparkContext sc;

    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("       欢迎使用 Spark 学习演示系统");
        System.out.println("=================================================");
        
        // 初始化 Spark
        initSpark();
        
        // 显示菜单
        showMenu();
        
        // 清理资源
        cleanup();
    }

    /**
     * 初始化 Spark
     */
    private static void initSpark() {
        System.out.println("\n正在初始化 Spark...");
        
        SparkConf conf = new SparkConf()
            .setAppName("Spark Learning Demo")
            .setMaster("local[*]")  // 本地模式，使用所有可用核心
            .set("spark.sql.shuffle.partitions", "4")  // 减少 shuffle 分区数
            .set("spark.default.parallelism", "4")     // 设置默认并行度
            // 解决 Java 17+ 模块访问问题
            .set("spark.driver.extraJavaOptions", 
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED " +
                "--add-opens java.base/java.lang.reflect=ALL-UNNAMED " +
                "--add-opens java.base/java.io=ALL-UNNAMED " +
                "--add-opens java.base/java.util=ALL-UNNAMED")
            .set("spark.executor.extraJavaOptions",
                "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
                "--add-opens java.base/java.lang=ALL-UNNAMED " +
                "--add-opens java.base/java.lang.reflect=ALL-UNNAMED " +
                "--add-opens java.base/java.io=ALL-UNNAMED " +
                "--add-opens java.base/java.util=ALL-UNNAMED");
        
        spark = SparkSession.builder()
                           .config(conf)
                           .getOrCreate();
        
        sc = new JavaSparkContext(spark.sparkContext());
        
        // 设置日志级别
        spark.sparkContext().setLogLevel("WARN");
        
        System.out.println("Spark 初始化完成!");
        System.out.println("Spark 版本: " + spark.version());
        System.out.println("运行模式: " + spark.sparkContext().master());
    }

    /**
     * 显示主菜单
     */
    private static void showMenu() {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        
        while (running) {
            System.out.println("\n=================================================");
            System.out.println("请选择要运行的演示:");
            System.out.println("=================================================");
            System.out.println("1.  RDD 操作演示");
            System.out.println("2.  DataFrame 和 Dataset 操作演示");
            System.out.println("3.  Spark SQL 演示");
            System.out.println("4.  数据读写演示");
            System.out.println("5.  高级特性演示");
            System.out.println("6.  运行所有演示");
            System.out.println("0.  退出");
            System.out.println("=================================================");
            System.out.print("请输入选项 (0-6): ");
            
            try {
                String input = scanner.nextLine().trim();
                int choice = Integer.parseInt(input);
                
                switch (choice) {
                    case 1:
                        runRDDDemo();
                        break;
                    case 2:
                        runDataFrameDemo();
                        break;
                    case 3:
                        runSparkSQLDemo();
                        break;
                    case 4:
                        runDataIODemo();
                        break;
                    case 5:
                        runAdvancedFeaturesDemo();
                        break;
                    case 6:
                        runAllDemos();
                        break;
                    case 0:
                        running = false;
                        System.out.println("\n感谢使用 Spark 学习演示系统！");
                        break;
                    default:
                        System.out.println("\n无效的选项，请重新选择！");
                }
            } catch (NumberFormatException e) {
                System.out.println("\n输入错误，请输入数字 0-6！");
            } catch (Exception e) {
                System.out.println("\n发生错误: " + e.getMessage());
                e.printStackTrace();
            }
            
            if (running) {
                System.out.println("\n按 Enter 键继续...");
                scanner.nextLine();
            }
        }
        
        scanner.close();
    }

    /**
     * 运行 RDD 操作演示
     */
    private static void runRDDDemo() {
        System.out.println("\n开始运行 RDD 操作演示...");
        System.out.println("=================================================");
        RDDOperationsDemo.runAllDemos(sc);
        System.out.println("=================================================");
        System.out.println("RDD 操作演示完成！");
    }

    /**
     * 运行 DataFrame 和 Dataset 演示
     */
    private static void runDataFrameDemo() {
        System.out.println("\n开始运行 DataFrame 和 Dataset 演示...");
        System.out.println("=================================================");
        DataFrameAndDatasetDemo.runAllDemos(spark);
        System.out.println("=================================================");
        System.out.println("DataFrame 和 Dataset 演示完成！");
    }

    /**
     * 运行 Spark SQL 演示
     */
    private static void runSparkSQLDemo() {
        System.out.println("\n开始运行 Spark SQL 演示...");
        System.out.println("=================================================");
        SparkSQLDemo.runAllDemos(spark);
        System.out.println("=================================================");
        System.out.println("Spark SQL 演示完成！");
    }

    /**
     * 运行数据读写演示
     */
    private static void runDataIODemo() {
        System.out.println("\n开始运行数据读写演示...");
        System.out.println("=================================================");
        DataIODemo.runAllDemos(spark);
        System.out.println("=================================================");
        System.out.println("数据读写演示完成！");
    }

    /**
     * 运行高级特性演示
     */
    private static void runAdvancedFeaturesDemo() {
        System.out.println("\n开始运行高级特性演示...");
        System.out.println("=================================================");
        AdvancedFeaturesDemo.runAllDemos(spark, sc);
        System.out.println("=================================================");
        System.out.println("高级特性演示完成！");
    }

    /**
     * 运行所有演示
     */
    private static void runAllDemos() {
        System.out.println("\n开始运行所有演示...");
        System.out.println("这可能需要几分钟时间，请耐心等待...\n");
        
        runRDDDemo();
        runDataFrameDemo();
        runSparkSQLDemo();
        runDataIODemo();
        runAdvancedFeaturesDemo();
        
        System.out.println("\n=================================================");
        System.out.println("所有演示已完成！");
        System.out.println("=================================================");
    }

    /**
     * 清理资源
     */
    private static void cleanup() {
        System.out.println("\n正在清理资源...");
        
        if (sc != null) {
            sc.close();
        }
        
        if (spark != null) {
            spark.stop();
        }
        
        System.out.println("资源清理完成！");
    }
}

