package com.ccgroup8.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;
import java.io.*;

public class WordLetterCount {
  // constant for our group output directory
  private static final String OUTPUT_DIR = "/test-data/CloudComputingCoursework_Group8";

  public static void main(String[] args) {
    String inputFile = parseArgs(args);

    SparkSession spark = SparkSession.builder()
        .appName("WordLetterCount")
        .getOrCreate();

    try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
      // Reads the input file into an RDD (Resilient Distributed Dataset) needed for
      // Spark distributed processing
      JavaRDD<String> lines = sc.textFile(inputFile);

      // Returns a list of words counting the occurrence of that word in lower case
      // and then ordering in descending order
      List<Tuple2<String, Integer>> wordList = countWords(lines);
      // Returns a list of tuples counting the character and the count in descending
      // order
      List<Tuple2<String, Integer>> letterList = countLetters(lines);

      // Use the categorise functions to get the common, popular and rare words and
      // lettters
      List<String[]> categorisedWords = categorise(wordList);
      List<String[]> categorisedLetters = categorise(letterList);

      new File(OUTPUT_DIR).mkdirs();
      // Write the categorised words and letters to CSV files
      writeCsv(OUTPUT_DIR + "/words_spark", categorisedWords);
      writeCsv(OUTPUT_DIR + "/letters_spark", categorisedLetters);

      // End the spark context
      sc.stop();
    }

    // Stop the Spark session
    spark.stop();
  }

  // I changed this to take positional args rather than hardcoded
  private static String parseArgs(String[] args) {
    for (int i = 0; i < args.length - 1; i++) {
      // Looks for the -i flag and returns the following argument which should be the
      // input file
      if (args[i].equals("-i")) {
        return args[i + 1];
      }
    }
    System.err.println("Usage: WordLetterCount -i <input file>");
    System.exit(1);
    return null;
  }

  private static List<Tuple2<String, Integer>> countWords(JavaRDD<String> lines) {
    // flatMap to work on each line and iterate through them and apply the function
    JavaPairRDD<String, Integer> wordCounts = lines
        .flatMap(line -> {
          // The required split to find the words in the line
          String[] tokens = line.split("[\\s,\\.;:\\?!\"\\(\\)\\[\\]\\{\\}\\-_]+");
          List<String> words = new ArrayList<>();
          for (String token : tokens) {
            token = token.toLowerCase();
            // loop through each word and make sure it consists of just letters (lower or
            // upper) add it to the Array list of words if so
            if (!token.isEmpty() && token.matches("[a-z]+")) {
              words.add(token);
            }
          }
          return words.iterator();
        })
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum);
    // Like with letters we map each word to an initial count of 1 in a tupe
    // Then aggregate the tuple by key in like a reduce to find the counts
    // Map and reduce

    List<Tuple2<String, Integer>> sorted = new ArrayList<>(wordCounts.collect());
    sortByFrequencyDesc(sorted);
    // Sort the words by frequency in descending order and return it
    return sorted;
  }

  private static List<Tuple2<String, Integer>> countLetters(JavaRDD<String> lines) {
    JavaPairRDD<String, Integer> letterCounts = lines
        .flatMap(line -> {
          List<String> letters = new ArrayList<>();
          // gets each character (c) in the array of characters obtained from the line
          // string
          for (char c : line.toCharArray()) {
            // Since the isCharacter takes accentented chars
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
              letters.add(String.valueOf(Character.toLowerCase(c)));
            }
          }
          return letters.iterator();
        })
        .mapToPair(letter -> new Tuple2<>(letter, 1))
        .reduceByKey(Integer::sum);

    // mapToPair creates a pair where the key is the character and we initialise the
    // count to 1
    // reduceByKey then sums up the count for all line, so we have total count of
    // each letter for whole file.
    // Example of map and reduce

    List<Tuple2<String, Integer>> sorted = new ArrayList<>(letterCounts.collect());
    sortByFrequencyDesc(sorted);
    // sorted sorts this list of tuples to sort the characters by their frequency in
    // descending order
    return sorted;
  }

  private static void sortByFrequencyDesc(List<Tuple2<String, Integer>> list) {
    // take a list and sort the tuples by comparing the second element (_2)
    // If the counts are the same tie break with alphalexical order of the first
    // element
    list.sort((a, b) -> {
      int cmp = Integer.compare(b._2, a._2);
      if (cmp != 0)
        return cmp;
      return a._1.compareTo(b._1);
    });
  }

  private static List<String[]> categorise(List<Tuple2<String, Integer>> sortedList) {
    // get the size of the argument (sorted list)
    int n = sortedList.size();
    // Find the bounds for popular, common and rare based on the give percentages
    int popularEnd = (int) Math.ceil(0.05 * n);
    int commonLow = (int) Math.floor(0.475 * n);
    int commonHigh = (int) Math.ceil(0.525 * n);
    int rareStart = (int) Math.floor(0.95 * n);

    // Make these bounds 0 indexed
    int popularIdxEnd = popularEnd - 1;
    int commonIdxLow = commonLow - 1;
    int commonIdxHigh = commonHigh - 1;
    int rareIdxStart = rareStart - 1;

    List<String[]> popular = new ArrayList<>();
    List<String[]> common = new ArrayList<>();
    List<String[]> rare = new ArrayList<>();

    // iterate through the sorted list
    for (int i = 0; i < n; i++) {
      // Get the ith tuple element
      Tuple2<String, Integer> entry = sortedList.get(i);
      int rank = i + 1;
      // We make a row with the rank, the word, the category and the frequency
      String[] row = { String.valueOf(rank), entry._1, null, String.valueOf(entry._2) };

      // If we haven't surpassed the popularEnd (most frequent) then mark as popular
      // otherwise if we are between the commonLow and commonHigh then mark as common
      // otherwise mark as rare
      if (i <= popularIdxEnd) {
        row[2] = "popular";
        popular.add(row);
      } else if (i >= commonIdxLow && i <= commonIdxHigh) {
        row[2] = "common";
        common.add(row);
      } else if (i >= rareIdxStart) {
        row[2] = "rare";
        rare.add(row);
      }
    }

    // We then combine the three categories into a single list
    List<String[]> result = new ArrayList<>();
    result.addAll(popular);
    result.addAll(common);
    result.addAll(rare);
    return result;
  }

  private static void writeCsv(String path, List<String[]> rows) {
    // Write to the CSV file
    try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
      for (String[] row : rows) {
        pw.println(row[0] + "," + row[1] + "," + row[2] + "," + row[3]);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
