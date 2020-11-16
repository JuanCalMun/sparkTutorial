package com.sparkTutorial.rdd.nasaApacheWebLogs;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf sparkConf = new SparkConf().setAppName("unionLogProblem").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> julyFirstLogs = sparkContext.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sparkContext.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> logLines = headlessFile(julyFirstLogs).union(headlessFile(augustFirstLogs));

        logLines.sample(true, 0.1)
                .saveAsTextFile(Utils.NASA_UNION_OUT_PATH);
    }

    private static JavaRDD<String> headlessFile(JavaRDD<String> headedFile) {
        String header = headedFile.first();
        return headedFile.filter(line -> !line.equals(header));
    }
}
