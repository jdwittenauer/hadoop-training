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
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class FilterTradesTall {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		TradeDAOTall tradeDao = null;
		tradeDao = new TradeDAOTall(conf);
		System.out.println("Filter ...");

		if (args.length > 0 && args[0].equalsIgnoreCase("none")) {
			scanWithoutFilters(tradeDao);

		} 
		else if (args.length > 0 && args[0].equalsIgnoreCase("row")) {
			scanRowFilter(tradeDao);
		} 
		else if (args.length > 0 && args[0].equalsIgnoreCase("prefix")) {
			scanPrefixFilter(tradeDao);
		} 
		else if (args.length > 0 && args[0].equalsIgnoreCase("column")) {
			scanSingleColumnValueFilter(tradeDao);
		} 
		else if (args.length > 0 && args[0].equalsIgnoreCase("list")) {
			scanListFilter(tradeDao);
		} 
		else {
			System.out.println("no args ...");
			System.out.println("run with argument none, for no filter");
			System.out.println("run with argument prefix, for prefix filter");
			System.out.println("run with argument list, for list filter");
		}

		tradeDao.close();
	}

	public static List<Trade> scanWithoutFilters(TradeDAOTall tradeDao) throws Exception {
		System.out.println("Scan No Filter");
		List<Trade> trades = tradeDao.scanTradesNoFilter();
		printTrades(trades);

		return trades;
	}

	public static List<Trade> scanRowFilter(TradeDAOTall tradeDao) throws Exception {
		System.out.println("Filter trades containing row key string");
		long timeEpoch = TimeUtil.convertStringToEpoch("10/10/2013 00:00:00");
		long reverseTime = Long.MAX_VALUE - timeEpoch;
		System.out.println("10/10/2013 00:00:00 time epoch long  " 
			+ timeEpoch + " reverse epoch long " + reverseTime);
		String filterString = Long.toString(reverseTime).substring(0, 8);
		System.out.println("******filter row keys containing string " + filterString);
		List<Trade> trades = scanRowFilter(tradeDao, filterString);
        printTrades(trades);

		return trades;
	}

	public static List<Trade> scanRowFilter(TradeDAOTall tradeDao, String filterString) throws IOException {
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(filterString));
		List<Trade> trades = tradeDao.scanTradesWithFilter(filter);
        printTrades(trades);

		return trades;
	}

	public static List<Trade> scanPrefixFilter(TradeDAOTall tradeDao) throws Exception {
		System.out.println(" Prefix filter GOOG");
		Filter filter = new PrefixFilter(Bytes.toBytes("GOOG"));
		List<Trade> trades = tradeDao.scanTradesWithFilter(filter);

		return trades;
	}

	public static List<Trade> scanSingleColumnValueFilter(TradeDAOTall tradeDao) throws Exception {
		long volume = 8000l;
		System.out.println(" SingleColumnValueFilter filter volume " + volume);
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				TradeDAOTall.baseCF,
				TradeDAOTall.volumeCol,
				CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(volume)));
		List<Trade> trades =tradeDao.scanTradesWithFilter(filter);
        printTrades(trades);

		return trades;
	}

	public static List<Trade> scanListFilter(TradeDAOTall tradeDao) throws Exception {
		System.out.println("List Filter");
		long volume = 8000l;
		FilterList filterlist = new FilterList(Operator.MUST_PASS_ALL);
		System.out.println(" QualifierFilter volume");
		Filter qualfilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, 
			new BinaryComparator(TradeDAOTall.volumeCol));
		System.out.println(" ValueFilter " + volume);
		Filter valuefilter = new ValueFilter(CompareFilter.CompareOp.GREATER, 
			new BinaryComparator(Bytes.toBytes(volume)));
		filterlist.addFilter(qualfilter);
		filterlist.addFilter(valuefilter);
		List<Trade> trades =tradeDao.scanTradesWithFilter(filterlist);
        printTrades(trades);

		return trades;
	}

	public static void printTrades(List<Trade> trades) {
		System.out.println("Printing " + trades.size() + " trades.");

		for (Trade trade : trades) {
			System.out.println(trade);
		}
	}
}
