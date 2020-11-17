package com.sparkTutorial.rdd.commons;

import org.apache.ivy.util.DateUtil;

import java.util.Date;

public class Utils {

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    //    Build constants
    public static final String MASTER_LOCAL = "local";
    public static final String MASTER_LOCAL_2 = "local[2]";
    public static final String MASTER_LOCAL_STAR = "local[*]";

    //    Path constants
    private static final String AIRPORTS_OUT_PATH = "out/airports";
    public static final String AIRPORTS_IN_USA_OUT_PATH = AIRPORTS_OUT_PATH + "/in_usa";
    public static final String AIRPORTS_BY_LATITUDE_OUT_PATH = AIRPORTS_OUT_PATH + "/by_latitude";
    private static final String NASA_OUT_PATH = "out/nasa";
    public static final String NASA_UNION_OUT_PATH = NASA_OUT_PATH + "/union/" + DateUtil.format(new Date()) + ".csv";
    public static final String NASA_INTERSECT_OUT_PATH = NASA_OUT_PATH + "/intersect/" + DateUtil.format(new Date()) +
            ".csv";
    public static String DATE_TEXT_FILE_NAME = DateUtil.format(new Date()) + ".text";
    public static final String AIRPORTS_NOT_USA_FILTERED_OUT_PATH = AIRPORTS_OUT_PATH + "/filtered/" + DATE_TEXT_FILE_NAME;
    public static final String AIRPORTS_UPPERCASE_OUT_PATH = AIRPORTS_OUT_PATH + "/uppercase/" + DATE_TEXT_FILE_NAME;

    private Utils() {
    }
}
