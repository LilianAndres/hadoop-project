# Hadoop Project

This repository contains a set of **Hadoop MapReduce Java applications** designed to run on a Hadoop cluster.  
Each application performs a different distributed data processing task to run on text files.

## 📦 Applications

The following Hadoop jobs are included in this project:

| Module              | Description                                                                                                                                                              |
|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ngram-count`       | Count n-grams (sequences of N words) in large text datasets.                                                                                                             |
| `word-length-count` | Count words grouped by their length.                                                                                                                                     |
| `letter-count`      | Count letters in words that are longer than 4 letters and shorter than 10 letters.                                                                                       |
| `top-k`             | Compute the Top-K most frequent entities (e.g. words, letters, etc.). Entries must be given in the format (entity, count) so another job must be needed before this one. |
| `next-word`         | Compute the next word predictions for a given word.                                                                                                                      |
| `sentence-filter`   | Filter sentences including a given word.                                                                                                                                 |

> Note that each module is a separate Maven module under this repository.

## 🚀 Getting Started

Follow these steps to **clone the repo, build the JAR artifacts, and run the jobs on a Hadoop cluster**.

### Clone the repository

```bash
git clone https://github.com/LilianAndres/hadoop-project.git
cd hadoop-project
```

### Build the JAR files

This project uses **Maven**. For each module, build the JAR as follows:

```bash
cd <module name>
mvn clean package
```

After building, the JAR will be in the module’s `target/` directory.
It is also possible to build all the JAR files in once by running `mvn clean package` from the project root.

### Run the MapReduce Job in your Hadoop cluster

Use the hadoop jar command to run a job. Replace <module-jar> with the actual JAR file and <main-class> with the job’s main class.

```bash
hadoop jar target/<module-jar>.jar <main-class> <input path> <output path>
```

For example, if running the `ngram-count` Job:

```bash
hadoop jar target/ngram-count-1.0-SNAPSHOT.jar org.ensai.hadoop.NGramCountDriver input output
```

### Visualize the output files in HDFS

```bash
hadoop dfs -ls <output path>
hadoop dfs -cat <output path>/part-r-00000
```

## Notes

- Upload clean and pre-processed data to HDFS before running these jobs.
- Use `hadoop fs -rmr` to remove output directories before repeated runs.
- Ensure everything is working properly and Hadoop daemons are up using `jps`.