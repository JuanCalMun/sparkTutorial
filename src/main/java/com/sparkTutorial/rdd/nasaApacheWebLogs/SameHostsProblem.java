package com.sparkTutorial.rdd.nasaApacheWebLogs;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf sparkConf = new SparkConf().setAppName("intersectLogProblem").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> julyFirstLogs = sparkContext.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sparkContext.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> julyHosts = headlessFile(julyFirstLogs).map(line -> line.split("\t")[0]);
        JavaRDD<String> augustHosts = headlessFile(augustFirstLogs).map(line -> line.split("\t")[0]);

        JavaRDD<String> bothMonthsHosts = julyHosts.intersection(augustHosts);

        bothMonthsHosts.saveAsTextFile(Utils.NASA_INTERSECT_OUT_PATH);
    }

    private static JavaRDD<String> headlessFile(JavaRDD<String> headedFile) {
        String header = headedFile.first();
        return headedFile.filter(line -> !line.equals(header));
    }
}
