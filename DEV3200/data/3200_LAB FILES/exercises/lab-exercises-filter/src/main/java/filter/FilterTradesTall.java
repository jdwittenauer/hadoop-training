package filter;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterTradesTall {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        TradeDAOTall tradeDao = null;

        tradeDao = new TradeDAOTall(conf);

        System.out.println("Filter ...");
        if (args.length > 0 && args[0].equalsIgnoreCase("none")) {
            // Print the table without applying any filters
            scanWithoutFilters(tradeDao);

        } else if (args.length > 0 && args[0].equalsIgnoreCase("row")) {
            scanRowFilter(tradeDao);

        } else if (args.length > 0 && args[0].equalsIgnoreCase("prefix")) {
            scanPrefixFilter(tradeDao);

        } else if (args.length > 0 && args[0].equalsIgnoreCase("column")) {
            scanSingleColumnValueFilter(tradeDao);

        } else if (args.length > 0 && args[0].equalsIgnoreCase("list")) {
            scanListFilter(tradeDao);

        } else {
            System.out.println("no args ...");
            System.out.println("run with argument none, for no filter");
            System.out.println("run with argument prefix, for prefix filter");
            System.out.println("run with argument list, for list filter");

        }

        tradeDao.close();
    }

    public static List<Trade> scanWithoutFilters(TradeDAOTall tradeDao) throws Exception {
        System.out.println(" Scan No Filter ");
        List<Trade> trades = tradeDao.scanTradesNoFilter();
        printTrades(trades);
        return trades;
    }

    public static List<Trade> scanRowFilter(TradeDAOTall tradeDao) throws Exception {
        System.out.println(" Filter trades containing row key string ");
	// Using a filter to select specific rows
        // row key = symbol + delim + reverse time (long)
        // get time in epoch long
        long timeEpoch = TimeUtil.convertStringToEpoch("10/10/2013 00:00:00");
        // calculate reverse time
        long reverseTime = Long.MAX_VALUE - timeEpoch;
        System.out.println("10/10/2013 00:00:00 time epoch long  " + timeEpoch
                + " reverse epoch long " + reverseTime);
        // get time substring for date
        String filterString = Long.toString(reverseTime).substring(0, 8);
        System.out.println("******filter row keys containing string " + filterString);
        List<Trade> trades = scanRowFilter(tradeDao, filterString);
        return trades;
    }

    public static List<Trade> scanRowFilter(TradeDAOTall tradeDao, String filterString)
            throws IOException {
	// TODO 2 finish code
        // create a RowFilter with a CompareFilter.CompareOp.EQUAL 
        // and SubstringComparator to filter the input filterString
        Filter filter = null; // TODO 2
        // TODO 2 finish code in tradeDao.scanTradesWithFilter
        List<Trade> trades = tradeDao.scanTradesWithFilter(filter);
        return trades;
    }

    public static List<Trade> scanPrefixFilter(TradeDAOTall tradeDao) throws Exception {
        System.out.println(" Prefix filter GOOG");
        byte[] bytes = Bytes.toBytes("GOOG");
	// TODO 3 finish code
        // TODO 3 create  PrefixFilter(bytes);
        Filter filter = null; // TODO 3 
        List<Trade> trades = tradeDao.scanTradesWithFilter(filter);
        return trades;

    }

    public static List<Trade> scanSingleColumnValueFilter(TradeDAOTall tradeDao)
            throws Exception {
        long volume = 8000l;
        System.out.println(" SingleColumnValueFilter filter volume " + volume);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                TradeDAOTall.baseCF, // CF
                TradeDAOTall.volumeCol, // volume column
                CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(
                        Bytes.toBytes(volume)));
        List<Trade> trades = tradeDao.scanTradesWithFilter(filter);
        return trades;
    }

    public static List<Trade> scanListFilter(TradeDAOTall tradeDao) throws Exception {
        // TODO 4 finish code
        System.out.println("List Filter");
        long volume = 8000l;
        FilterList filterlist = new FilterList(Operator.MUST_PASS_ALL);
        System.out.println(" QualifierFilter volume");
	// create a QualifierFilter with CompareFilter.CompareOp.EQUAL
        // BinaryComparator(TradeDAOTall.volumeCol)
        Filter qualfilter = null; // TODO 4  finish code
        System.out.println(" ValueFilter " + volume);
        // create a ValueFilter for > BinaryComparator(Bytes.toBytes(volume))
        Filter valuefilter = null; // TODO 4  finish code
        filterlist.addFilter(qualfilter);
        filterlist.addFilter(valuefilter);
        List<Trade> trades = tradeDao.scanTradesWithFilter(filterlist);
        return trades;
    }

    public static void printTrades(List<Trade> trades) {
        System.out.println("Printing " + trades.size() + " trades.");
        for (Trade trade : trades) {
            System.out.println(trade);
        }
    }

}
