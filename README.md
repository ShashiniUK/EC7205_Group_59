# Online Retail Sales Analysis using Hadoop MapReduce

## Project Overview
This project performs large-scale data analysis on the [UCI Online Retail Dataset](https://archive.ics.uci.edu/dataset/352/online+retail) using Hadoop MapReduce. The goal is to calculate total revenue, number of transactions, and average revenue per transaction for each country.

### Description
This project demonstrates how Hadoop MapReduce can be used to efficiently process and analyze real-world transaction data. By implementing custom Mapper and Reducer classes, the program extracts insights such as total sales and transaction counts across countries. It highlights the power of distributed computing in analyzing large-scale retail datasets.

## Project Structure
```
SalesAnalysis/
├── CountryRevenueWritable.java
├── SalesMapper.java
├── SalesReducer.java
├── SalesAnalysisDriver.java
```

## Dataset
- **Name:** Online Retail Dataset
- **Source:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/352/online+retail)
- **Description:** Contains transactions from a UK-based online store between December 2010 and December 2011.
- **Format:** CSV

## Prerequisites
- Java 8 or above
- Apache Hadoop (version 2.7+)
- Maven (for building the JAR)

## Compilation Instructions
1. Create a new Maven project or compile manually using `javac`.
2. If using Maven, include the Hadoop dependencies in your `pom.xml`.
3. Compile and package into a JAR file:

```bash
mvn clean package
```

4. The resulting JAR will be located in `target/retailanalysis-1.0-SNAPSHOT.jar`

## Running the Job
1. Upload the dataset to HDFS:
```bash
hdfs dfs -mkdir /user/retail/input
hdfs dfs -put OnlineRetail.csv /user/retail/input
```

2. Run the MapReduce job:
```bash
hadoop jar retailanalysis-1.0-SNAPSHOT.jar SalesAnalysisDriver /user/retail/input /user/retail/output
```

3. View the output:
```bash
hdfs dfs -cat /user/retail/output/part-r-00000
```

## Output Format
Each line contains:
```
<Country>\t<Total Revenue>\t<Transaction Count>\t<Average Revenue per Transaction>
```
Example:
```
United Kingdom	902522.08	485123	18.60
Singapore	21279.29	222	95.85
```

## Author
- Team [Insert Group Name]

## License
Apache 2.0
