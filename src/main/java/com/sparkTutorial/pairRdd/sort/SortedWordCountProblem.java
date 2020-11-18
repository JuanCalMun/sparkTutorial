package com.sparkTutorial.pairRdd.sort;


import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SortedWordCountProblem {

    public static void main(String[] args) {
    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setMaster(Utils.MASTER_LOCAL).setAppName("sortedWordCountProblem");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("in/word_count.text");
        JavaPairRDD<String, Integer> words = lines
                .flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> wordsSortedByUse = words
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false);

        wordsSortedByUse.collect().forEach(tuple -> System.out.println(tuple._2 + " : " + tuple._1));

    }


}

