package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.ivy.util.DateUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

public class AirportsInUsaProblem {

    public static final String LOCAL_2 = "local[2]";

    public static void main(String[] args) {

        /*  Create a Spark program to read the airport data from in/airports.text, find all the airports which are
            located in United States and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */

        SparkConf sparkConf = new SparkConf().setAppName("airports").setMaster(LOCAL_2);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        JavaRDD<String> airports = sparkContext.textFile("in/airports.text");
        JavaRDD<String> unitedStatesAirports =
                airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));

        JavaRDD<String> outputUsaStatesAirports = unitedStatesAirports.map(line -> {
            String[] column = line.split(Utils.COMMA_DELIMITER);
            return column[1] + "," + column[2];
        });

        String fileName = DateUtil.format(new Date()) + ".text";
        String outputPath = Utils.AIRPORTS_IN_USA_OUT_PATH + "/" + fileName;
        outputUsaStatesAirports.saveAsTextFile(outputPath);
    }
}
