package com.ccgroup8.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
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

  }
}
