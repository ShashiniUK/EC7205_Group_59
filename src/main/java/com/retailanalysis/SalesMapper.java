package com.retailanalysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for Online Retail Sales Analysis
 * Processes each transaction record and extracts country, quantity, and unit price
 * Filters out cancelled transactions (InvoiceNo starting with 'C')
 */
public class SalesMapper extends Mapper<LongWritable, Text, Text, CountryRevenueWritable> {

    private Text country = new Text();
    private CountryRevenueWritable revenueData = new CountryRevenueWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Skip header line
        if (key.get() == 0 && value.toString().startsWith("InvoiceNo")) {
            return;
        }

        try {
            // Parse CSV line - handle commas within quoted fields
            String[] fields = parseCSVLine(value.toString());

            // Validate minimum required fields
            if (fields.length < 8) {
                context.getCounter("SALES_MAPPER", "INVALID_RECORD_LENGTH").increment(1);
                return;
            }

            String invoiceNo = fields[0].trim();
            String stockCode = fields[1].trim();
            String description = fields[2].trim();
            String quantityStr = fields[3].trim();
            String invoiceDateStr = fields[4].trim();
            String unitPriceStr = fields[5].trim();
            String customerIdStr = fields[6].trim();
            String countryStr = fields[7].trim();

            // Filter out cancelled transactions (InvoiceNo starts with 'C')
            if (invoiceNo.startsWith("C") || invoiceNo.startsWith("c")) {
                context.getCounter("SALES_MAPPER", "CANCELLED_TRANSACTIONS").increment(1);
                return;
            }

            // Validate required fields are not empty
            if (countryStr.isEmpty() || quantityStr.isEmpty() || unitPriceStr.isEmpty()) {
                context.getCounter("SALES_MAPPER", "MISSING_REQUIRED_FIELDS").increment(1);
                return;
            }

            // Parse quantity and unit price
            int quantity;
            double unitPrice;

            try {
                quantity = Integer.parseInt(quantityStr);
                unitPrice = Double.parseDouble(unitPriceStr);
            } catch (NumberFormatException e) {
                context.getCounter("SALES_MAPPER", "INVALID_NUMERIC_VALUES").increment(1);
                return;
            }

            // Skip records with negative quantity or zero/negative price
            if (quantity <= 0 || unitPrice <= 0) {
                context.getCounter("SALES_MAPPER", "INVALID_QUANTITY_OR_PRICE").increment(1);
                return;
            }

            // Calculate revenue
            double revenue = quantity * unitPrice;

            // Set output key-value pair
            country.set(countryStr);
            revenueData = new CountryRevenueWritable(revenue, 1);

            // Emit country and revenue data
            context.write(country, revenueData);
            context.getCounter("SALES_MAPPER", "VALID_TRANSACTIONS_PROCESSED").increment(1);

        } catch (Exception e) {
            context.getCounter("SALES_MAPPER", "PROCESSING_ERRORS").increment(1);
            // Log error but continue processing
            System.err.println("Error processing record: " + value.toString() +
                    " Error: " + e.getMessage());
        }
    }

    /**
     * Parse CSV line handling commas within quoted fields
     * Simple implementation - for production use, consider libraries like OpenCSV
     */
    private String[] parseCSVLine(String line) {
        // This is a simplified CSV parser
        // For more robust parsing, consider using libraries like Apache Commons CSV

        // If no quotes, simple split
        if (!line.contains("\"")) {
            return line.split(",", -1);
        }

        // Handle quoted fields with embedded commas
        java.util.List<String> fields = new java.util.ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentField = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '\"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString());
                currentField.setLength(0);
            } else {
                currentField.append(c);
            }
        }

        // Add the last field
        fields.add(currentField.toString());

        return fields.toArray(new String[0]);
    }
}