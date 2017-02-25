/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package social;

import java.util.Date;

/**
 *
 * @author carol
 */
public class TimeUtil {

    public static String dateToReverseString(Date date) {
        String reversedDateAsStr =
                Long.toString(Long.MAX_VALUE - date.getTime());
        StringBuilder builder = new StringBuilder();

        for (int i = reversedDateAsStr.length(); i < 19; i++) {
            builder.append('0');
        }
        builder.append(reversedDateAsStr);
        return builder.toString();
    }

    public static String dateToString(Date date) {
        String dateAsStr =
                Long.toString(date.getTime());
        StringBuilder builder = new StringBuilder();
        for (int i = dateAsStr.length(); i < 19; i++) {
            builder.append('0');
        }
        builder.append(dateAsStr);
        return builder.toString();
    }

    public static Date stringToDate(String column) {
        long stamp = Long.parseLong(column);
        return new Date(stamp);
    }

    public static Date reverseStringToDate(String column) {
        long reverseStamp = Long.parseLong(column);
        return new Date(Long.MAX_VALUE - reverseStamp);
    }

    public static long getEpoch() {
        long epoch = System.currentTimeMillis() / 1000;
        return epoch;
    }

    public static long reverseEpoch() {
        long epoch = Long.MAX_VALUE - getEpoch();
        return epoch;
    }

    public static long convertStringToEpoch(String time) throws Exception {
        // format  "01/01/1970 01:00:00"
        long epoch = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse(time).getTime() / 1000;
        return epoch;
    }

    public static String convertEpochToString(long epoch) throws Exception {
        // format  "01/01/1970 01:00:00"
        String date = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(epoch * 1000));
        return date;
    }
}
