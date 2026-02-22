package com.ccgroup8.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;
import java.io.*;

public class WordLetterCount {
    private static final String OUTPUT_DIR = "/test-data/CloudComputingCoursework_Group8";

    public static void main(String[] args) {
        String inputFile = parseArgs(args);

        SparkSession spark = SparkSession.builder()
            .appName("WordLetterCount")
            .getOrCreate();


        try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {
            JavaRDD<String> lines = sc.textFile(inputFile);

            List<Tuple2<String, Integer>> wordList = countWords(lines);
            List<Tuple2<String, Integer>> letterList = countLetters(lines);

            List<String[]> categorisedWords = categorise(wordList);
            List<String[]> categorisedLetters = categorise(letterList);

            new File(OUTPUT_DIR).mkdirs();
            writeCsv(OUTPUT_DIR + "/words_spark", categorisedWords);
            writeCsv(OUTPUT_DIR + "/letters_spark", categorisedLetters);

            sc.stop();
        }

        spark.stop();
    }

    // I changed this to take positional args rather than hardcoded
    private static String parseArgs(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("-i")) {
                return args[i + 1];
            }
        }
        System.err.println("Usage: WordLetterCount -i <input file>");
        System.exit(1);
        return null;
    }

    private static List<Tuple2<String, Integer>> countWords(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> wordCounts = lines
            .flatMap(line -> {
                // The required split for finding words
                String[] tokens = line.split("[\\s,\\.;:\\?!\"\\(\\)\\[\\]\\{\\}\\-_]+");
                List<String> words = new ArrayList<>();
                for (String token : tokens) {
                    if (!token.isEmpty() && token.matches("[a-zA-Z]+")) {
                        words.add(token.toLowerCase());
                    }
                }
                return words.iterator();
            })
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> sorted = new ArrayList<>(wordCounts.collect());
        sortByFrequencyDesc(sorted);
        return sorted;
    }

    private static List<Tuple2<String, Integer>> countLetters(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> letterCounts = lines
            .flatMap(line -> {
                List<String> letters = new ArrayList<>();
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

        List<Tuple2<String, Integer>> sorted = new ArrayList<>(letterCounts.collect());
        sortByFrequencyDesc(sorted);
        return sorted;
    }

    private static void sortByFrequencyDesc(List<Tuple2<String, Integer>> list) {
        list.sort((a, b) -> {
            int cmp = Integer.compare(b._2, a._2);
            if (cmp != 0) return cmp;
            return a._1.compareTo(b._1);
        });
    }

    private static List<String[]> categorise(List<Tuple2<String, Integer>> sortedList) {
        int n = sortedList.size();
        int popularEnd = (int) Math.ceil(0.05 * n);
        int commonLow = (int) Math.floor(0.475 * n);
        int commonHigh = (int) Math.ceil(0.525 * n);
        int rareStart = (int) Math.floor(0.95 * n);

        List<String[]> popular = new ArrayList<>();
        List<String[]> common = new ArrayList<>();
        List<String[]> rare = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            Tuple2<String, Integer> entry = sortedList.get(i);
            int rank = i + 1;
            String[] row = {String.valueOf(rank), entry._1, null, String.valueOf(entry._2)};

            if (i < popularEnd) {
                row[2] = "popular";
                popular.add(row);
            } else if (i >= commonLow - 1 && i <= commonHigh - 1) {
                row[2] = "common";
                common.add(row);
            } else if (i >= rareStart - 1) {
                row[2] = "rare";
                rare.add(row);
            }
        }

        List<String[]> result = new ArrayList<>();
        result.addAll(popular);
        result.addAll(common);
        result.addAll(rare);
        return result;
    }

    private static void writeCsv(String path, List<String[]> rows) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            for (String[] row : rows) {
                pw.println(row[0] + "," + row[1] + "," + row[2] + "," + row[3]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
