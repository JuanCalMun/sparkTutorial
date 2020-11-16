package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose
        latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           0 Airport ID, 1 Name of airport, 2 Main city served by airport, 3 Country where airport is located,
           4 IATA/FAA code, 5 ICAO Code, 6 Latitude, 7 Longitude, 8 Altitude, 9 Timezone, 10 DST, 11 Timezone in
           Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
              ...
         */


        SparkConf sparkConf = new SparkConf().setAppName("airportsByLatitude").setMaster(Utils.MASTER);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> airports = sparkContext.textFile("in/airports.text");
        JavaRDD<String> airportsFilteredByLatitude =
                airports.filter(airport -> Float.parseFloat(airport.split(Utils.COMMA_DELIMITER)[6]) > 40).map(airport -> {
            String[] values = airport.split(Utils.COMMA_DELIMITER);
            return values[1] + ", " + values[6];
        });

        String outputPath = Utils.AIRPORTS_BY_LATITUDE_OUT_PATH + "/" + Utils.DATE_TEXT_FILE_NAME;
        airportsFilteredByLatitude.saveAsTextFile(outputPath);

    }
}
