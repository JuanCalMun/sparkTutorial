package com.sparkTutorial.rdd.commons;

import org.apache.ivy.util.DateUtil;

import java.util.Date;

public class Utils {

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    //    Build constants
    public static final String MASTER = "local[2]";

    //    Path constants
    private static final String OUT_PATH = "out";

    private static final String AIRPORTS_OUT_PATH = OUT_PATH + "/airports";

    public static final String AIRPORTS_IN_USA_OUT_PATH = AIRPORTS_OUT_PATH + "/in_usa";
    public static final String AIRPORTS_BY_LATITUDE_OUT_PATH = AIRPORTS_OUT_PATH + "/by_latitude";

    public static String DATE_TEXT_FILE_NAME = DateUtil.format(new Date()) + ".text";
    

    private Utils() {
    }
}
