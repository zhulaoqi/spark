package com.spark.learning;

import com.spark.learning.demo.*;

/**
 * 统一作业调度入口
 * 
 * 使用方式:
 * spark-submit --class com.spark.learning.JobDispatcher your.jar <job-name> [args]
 */
public class JobDispatcher {
    
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("用法: JobDispatcher <job-name> [job-args]");
            System.err.println();
            System.err.println("可用的 job-name:");
            System.err.println("  rdd           - RDD 操作演示");
            System.err.println("  dataframe     - DataFrame/Dataset 演示");
            System.err.println("  sql           - Spark SQL 演示");
            System.err.println("  io            - 数据读写演示");
            System.err.println("  advanced      - 高级特性演示");
            System.err.println("  interactive   - 交互式学习（默认）");
            System.exit(1);
        }
        
        String jobName = args[0].toLowerCase();
        
        try {
            switch (jobName) {
                case "rdd":
                    RDDOperationsDemo.main(new String[]{});
                    break;
                    
                case "dataframe":
                    DataFrameAndDatasetDemo.main(new String[]{});
                    break;
                    
                case "sql":
                    SparkSQLDemo.main(new String[]{});
                    break;
                    
                case "io":
                    DataIODemo.main(new String[]{});
                    break;
                    
                case "advanced":
                    AdvancedFeaturesDemo.main(new String[]{});
                    break;
                    
                case "interactive":
                default:
                    SparkLearningMain.main(new String[]{});
                    break;
            }
        } catch (Exception e) {
            System.err.println("执行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

