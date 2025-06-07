
# Online Retail Sales Analysis using Hadoop MapReduce

## Project Overview
This project performs large-scale data analysis on the [UCI Online Retail Dataset](https://archive.ics.uci.edu/dataset/352/online+retail) using Hadoop MapReduce. The goal is to calculate total revenue, number of transactions, and average revenue per transaction for each country.

### Description
This project demonstrates how Hadoop MapReduce can be used to efficiently process and analyze real-world transaction data. By implementing custom Mapper and Reducer classes, the program extracts insights such as total sales and transaction counts across countries. It highlights the power of distributed computing in analyzing large-scale retail datasets.



## Dataset
- **Name:** Online Retail Dataset
- **Source:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/352/online+retail)
- **Description:** Contains transactions from a UK-based online store between December 2010 and December 2011.
- **Format:** CSV

## Prerequisites
- Java 8
- Apache Hadoop 3.4.x
- Maven
- WSL or Linux shell environment

## Compilation Instructions
1. Ensure Java 8 is installed:
   ```bash
   sudo apt install openjdk-8-jdk
   java -version
   ```

2. Build the JAR file using Maven:
   ```bash
   mvn clean package
   ```

3. The JAR file will be located at:
   ```
   target/retailanalysis-1.0-SNAPSHOT.jar
   ```

## Running the Job

### Step 1: Create HDFS directories
```bash
hdfs dfs -mkdir -p /user/yourusername/input
hdfs dfs -mkdir -p /user/yourusername/output
```

### Step 2: Upload dataset to HDFS
```bash
hdfs dfs -put data/OnlineRetail.csv /user/yourusername/input
```

### Step 3: Execute the MapReduce job
```bash
hadoop jar target/retailanalysis-1.0-SNAPSHOT.jar com.retailanalysis.SalesAnalysisDriver /user/yourusername/input /user/yourusername/output
```

### Step 4: View the output
```bash
hdfs dfs -cat /user/yourusername/output/part-r-00000
```

## Output Format
Each output line contains:
```
<Country>	<Total Revenue>	<Transaction Count>	<Average Revenue per Transaction>
```

Example:
```
United Kingdom	902522.08	485123	18.60
Netherlands	285446.34	2359	121.00
Singapore	21279.29	222	95.85
```

## Notes
- Ensure all Hadoop daemons are running before executing the job:
  ```bash
  ./sbin/start-all.sh
  jps  # to verify NameNode, DataNode, ResourceManager, etc.
  ```
- Output is stored in HDFS under `/user/yourusername/output`.

## Author
- Team Group 59
      EG/2020/3945	Gunarathna K.M.W.G.S.L
      EG/2020/4030	Kumanayake H.P.
      EG/2020/4247	Udayanthika K.D.S.



