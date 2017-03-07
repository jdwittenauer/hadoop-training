package filter;

import java.util.Date;


public class TimeUtil {
    public static String dateToReverseString(Date date) {
        String reversedDateAsStr = Long.toString(Long.MAX_VALUE - date.getTime());
        StringBuilder builder = new StringBuilder();

        for (int i = reversedDateAsStr.length(); i < 19; i++) {
            builder.append('0');
        }

        builder.append(reversedDateAsStr);
        return builder.toString();
    }

    public static String dateToString(Date date) {
        String dateAsStr = Long.toString(date.getTime());
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
        long epoch = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").parse(time).getTime();
        return epoch;
    }

    public static String convertEpochToString(long epoch) throws Exception {
        String date = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(epoch));
        return date;
    }
}
