package com.sparkTutorial.rdd.sumOfNumbers;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.parquet.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SumOfNumbersProblem {

    public static void main(String[] args) {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

        SparkConf sparkConf = new SparkConf().setAppName("sumOfNumbers").setMaster(Utils.MASTER_LOCAL_2);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("in/prime_nums.text");
        JavaRDD<Integer> numbers =
                lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                        .filter(number -> !Strings.isNullOrEmpty(number))
                        .map(Integer::valueOf);
        Integer sum = numbers.reduce(Integer::sum);
        System.out.println("The sum of the numbers is: " + sum);

    }
}
