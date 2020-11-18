package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsByCountryProblem {

    public static void main(String[] args) {

        /* Create a Spark program to read the airport data from in/airports.text,
           output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("airportsByCountryProblem").setMaster(Utils.MASTER_LOCAL);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("in/airports.text");
        JavaPairRDD<String, Iterable<String>> airportsByCountry =
                lines.map(line -> line.split(Utils.COMMA_DELIMITER))
                        .mapToPair(columns -> new Tuple2<>(columns[3], columns[1]))
                        .groupByKey();

        airportsByCountry.foreach((e) -> System.out.println(e._1 + ": " + e._2().toString()));


    }
}
