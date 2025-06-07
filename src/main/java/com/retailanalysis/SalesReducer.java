package com.retailanalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for Online Retail Sales Analysis
 * Aggregates revenue and transaction counts by country
 */
public class SalesReducer extends Reducer<Text, CountryRevenueWritable, Text, CountryRevenueWritable> {

    private CountryRevenueWritable result = new CountryRevenueWritable();

    @Override
    public void reduce(Text key, Iterable<CountryRevenueWritable> values, Context context)
            throws IOException, InterruptedException {

        double totalRevenue = 0.0;
        long totalTransactions = 0L;

        // Aggregate all revenue data for this country
        for (CountryRevenueWritable value : values) {
            totalRevenue += value.getTotalRevenue();
            totalTransactions += value.getTransactionCount();
        }

        // Create result object
        result = new CountryRevenueWritable(totalRevenue, totalTransactions);

        // Emit country and aggregated revenue data
        context.write(key, result);

        // Update counters for monitoring
        context.getCounter("SALES_REDUCER", "COUNTRIES_PROCESSED").increment(1);
        context.getCounter("SALES_REDUCER", "TOTAL_REVENUE_CENTS").increment((long)(totalRevenue * 100));
        context.getCounter("SALES_REDUCER", "TOTAL_TRANSACTIONS").increment(totalTransactions);
    }
}