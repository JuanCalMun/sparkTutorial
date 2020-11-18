package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;


import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AverageHousePriceProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setMaster(Utils.MASTER_LOCAL).setAppName("averageHousePriceProblem");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("in/RealEstate.csv");
        JavaPairRDD<Integer, AvgCount> priceByBedrooms =
                lines.filter(line -> !line.contains("Bedrooms"))
                        .map(line -> line.split(","))
                        .mapToPair(values -> new Tuple2<>(
                                Integer.valueOf(values[3]),
                                new AvgCount(1, Double.parseDouble(values[2]))));

        JavaPairRDD<Integer, AvgCount> priceByBedroomsTotal = priceByBedrooms
                .reduceByKey((x, y) -> new AvgCount(
                        x.getCount() + y.getCount(),
                        x.getTotal() + y.getTotal()));

        JavaPairRDD<Integer, Double> priceByBedroomsMean =
                priceByBedroomsTotal.mapValues(avgCount -> avgCount.getTotal() / avgCount.getCount());

        System.out.println("housePriceTotal: ");
        priceByBedroomsTotal.collectAsMap()
                .forEach((count, value) -> System.out.println(count + " : " + value));

        System.out.println("housePriceMean: ");
        priceByBedroomsMean.collectAsMap()
                .forEach((count, value) -> System.out.println(count + " : " + value));

    }

}
