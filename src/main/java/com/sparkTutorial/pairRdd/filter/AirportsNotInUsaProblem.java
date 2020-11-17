package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */

        SparkConf sparkConf = new SparkConf().setAppName("airportsNotInUsa").setMaster(Utils.MASTER_LOCAL_2);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("in/airports.text");

        JavaPairRDD<String, String> airports = lines
                .map(line -> line.split(Utils.COMMA_DELIMITER))
                .mapToPair(elements -> new Tuple2<>(elements[1], elements[3]));

        JavaPairRDD<String, String> notUsaAirports = airports.filter(airport -> !"\"United States\"".equals(airport._2));
        notUsaAirports.saveAsTextFile(Utils.AIRPORTS_NOT_USA_FILTERED);

    }
}
