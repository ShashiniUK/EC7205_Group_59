package com.retailanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Driver class for Online Retail Sales Revenue Analysis
 * MapReduce job to calculate total revenue and transaction counts by country
 *
 * Usage: hadoop jar sales-analysis.jar SalesAnalysisDriver <input_path> <output_path>
 */
public class SalesAnalysisDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // Parse command line arguments
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: SalesAnalysisDriver <input_path> <output_path>");
            System.err.println("Example: hadoop jar sales-analysis.jar SalesAnalysisDriver /input/data.csv /output");
            System.exit(2);
        }

        // Configure the job
        Job job = Job.getInstance(conf, "online-retail-sales-analysis");
        job.setJarByClass(SalesAnalysisDriver.class);

        // Set mapper and reducer classes
        job.setMapperClass(SalesMapper.class);
        job.setCombinerClass(SalesReducer.class);  // Reducer can be used as combiner
        job.setReducerClass(SalesReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CountryRevenueWritable.class);

        // Set input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Configure job properties for optimization
        job.getConfiguration().set("mapreduce.map.memory.mb", "1024");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "2048");
        job.getConfiguration().set("mapreduce.task.timeout", "600000"); // 10 minutes

        // Print job configuration
        System.out.println("=== Online Retail Sales Analysis Job Configuration ===");
        System.out.println("Input path: " + otherArgs[0]);
        System.out.println("Output path: " + otherArgs[1]);
        System.out.println("Job name: " + job.getJobName());
        System.out.println("======================================================");

        // Submit job and wait for completion
        boolean success = job.waitForCompletion(true);

        // Print job statistics
        if (success) {
            System.out.println("\n=== Job Completed Successfully ===");
            System.out.println("Job ID: " + job.getJobID());
            System.out.println("Job Tracking URL: " + job.getTrackingURL());

            // Print counters
            System.out.println("\n=== Job Statistics ===");
            System.out.println("Mapper Counters:");
            System.out.println("  Valid transactions processed: " +
                    job.getCounters().findCounter("SALES_MAPPER", "VALID_TRANSACTIONS_PROCESSED").getValue());
            System.out.println("  Cancelled transactions: " +
                    job.getCounters().findCounter("SALES_MAPPER", "CANCELLED_TRANSACTIONS").getValue());
            System.out.println("  Invalid records: " +
                    job.getCounters().findCounter("SALES_MAPPER", "INVALID_RECORD_LENGTH").getValue());
            System.out.println("  Processing errors: " +
                    job.getCounters().findCounter("SALES_MAPPER", "PROCESSING_ERRORS").getValue());

            System.out.println("\nReducer Counters:");
            System.out.println("  Countries processed: " +
                    job.getCounters().findCounter("SALES_REDUCER", "COUNTRIES_PROCESSED").getValue());
            System.out.println("  Total transactions: " +
                    job.getCounters().findCounter("SALES_REDUCER", "TOTAL_TRANSACTIONS").getValue());
            System.out.println("  Total revenue (£): " +
                    String.format("%.2f", job.getCounters().findCounter("SALES_REDUCER", "TOTAL_REVENUE_CENTS").getValue() / 100.0));

            System.out.println("\n=== Results Location ===");
            System.out.println("Output available at: " + otherArgs[1] + "/part-r-00000");
            System.out.println("Use: hdfs dfs -cat " + otherArgs[1] + "/part-r-00000");
            System.out.println("========================");
        } else {
            System.err.println("Job failed!");
        }

        System.exit(success ? 0 : 1);
    }
}