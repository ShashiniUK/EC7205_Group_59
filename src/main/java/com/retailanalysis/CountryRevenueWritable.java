package com.retailanalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Custom Writable class to store revenue and transaction count for each country
 */
public class CountryRevenueWritable implements Writable {

    private double totalRevenue;
    private long transactionCount;

    // Default constructor required for Hadoop serialization
    public CountryRevenueWritable() {
        this.totalRevenue = 0.0;
        this.transactionCount = 0L;
    }

    public CountryRevenueWritable(double totalRevenue, long transactionCount) {
        this.totalRevenue = totalRevenue;
        this.transactionCount = transactionCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(totalRevenue);
        out.writeLong(transactionCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalRevenue = in.readDouble();
        transactionCount = in.readLong();
    }

    public void addRevenue(double revenue) {
        this.totalRevenue += revenue;
        this.transactionCount++;
    }

    public void add(CountryRevenueWritable other) {
        this.totalRevenue += other.totalRevenue;
        this.transactionCount += other.transactionCount;
    }

    // Getters
    public double getTotalRevenue() {
        return totalRevenue;
    }

    public long getTransactionCount() {
        return transactionCount;
    }

    public double getAverageTransactionValue() {
        return transactionCount > 0 ? totalRevenue / transactionCount : 0.0;
    }

    @Override
    public String toString() {
        return String.format("%.2f\t%d\t%.2f",
                totalRevenue, transactionCount, getAverageTransactionValue());
    }
}