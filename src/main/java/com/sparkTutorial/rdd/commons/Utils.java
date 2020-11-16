package com.sparkTutorial.rdd.commons;

public class Utils {

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static final String OUT_PATH = "out";
    public static final String AIRPORTS_OUT_PATH = OUT_PATH + "/airports";
    public static final String AIRPORTS_IN_USA_OUT_PATH = AIRPORTS_OUT_PATH + "/in_usa";

    private Utils() {
    }
}
