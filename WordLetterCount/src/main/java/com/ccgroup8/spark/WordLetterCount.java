package com.ccgroup8.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import java.util.*;

public class WordLetterCount {
  public static void main(String[] args) {
    // Ensure that the argument is provided and we don't have extra arguments
    if (args.length < 1 || args.length > 2) {
      System.err.println("Usage: WordLetterCount <input_file>");
      System.exit(1);
    }

    // Obtain the I/O points
    String inputFile = args[0];
    String outputDir = "/test-data/CloudComputingCoursework_Group8";

    // Create a Spark session
    SparkSession spark = SparkSession.builder().appName("Word Letter Count").getOrCreate();

    // Uses the spark session to interact with the file system and read the input
    // file returning a Dataset<String> where each row is a line from the input file
    Dataset<String> file_table = spark.read().textFile(inputFile);
    // Convert to RDD (resilient distributed dataset) to distribute storage across
    // the cluster allowing for parallel processing and resilience
    JavaRDD<String> lines = file_table.toJavaRDD();

    // Use flatmap to convert the input line into words by creating a new function
    // that takes a line input string and returns an iterator of the words in the
    // line
    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      // override the FlatMapFunction interface to implement a method used by Spark
      // called "call" that takes a line and returns an iterator of words
      @Override
      public Iterator<String> call(String line) {
        // Turns the line into lowercase
        String lower = line.toLowerCase();

        // Regex to split words using the specification provided
        String regex = "[\\s,.;:?!\"()\\[\\]{}\\-_]+";

        // Split the line into words using the regex to create an array of strings
        String[] tkns = lower.split(regex);

        // Convert to a List<String> for easier manipulation
        List<String> results = new ArrayList<>();

        for (String word : tkns) {
          results.add(word);
        }

        // Return the iterator for the list
        return results.iterator();
      }
    });

  }
}
