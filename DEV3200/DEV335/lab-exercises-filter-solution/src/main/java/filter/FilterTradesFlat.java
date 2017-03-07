package filter;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class FilterTradesFlat {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		TradeDAOFlat tradeDao = null;
		tradeDao = new TradeDAOFlat(conf);
		System.out.println("Filter ...");

		if (args.length > 0 && args[0].equalsIgnoreCase("none")) {
			scanWithoutFilters(tradeDao);
		} 
		else if (args.length > 0 && args[0].equalsIgnoreCase("prefix")) {
			scanPrefixFilter(tradeDao);
		} 
		else if (args.length > 0 && args[0].equalsIgnoreCase("value")) {
			scanValueFilter(tradeDao);
		} 
		else if (args.length > 0 && args[0].equalsIgnoreCase("column")) {
			scanColumnFilter(tradeDao);
		} 
		else if (args.length > 0 && args[0].equalsIgnoreCase("list")) {
			scanListFilter(tradeDao);
		} 
		else {
			System.out.println("no args ...");
			scanWithoutFilters(tradeDao);
		}

		tradeDao.close();
	}

	public static void scanWithoutFilters(TradeDAOFlat tradeDao) throws Exception {
		System.out.println("Scan No Filter");
		List<Trade> trades = tradeDao.scanTradesNoFilter();
		printTrades(trades);
	}

	public static void scanValueFilter(TradeDAOFlat tradeDao) throws Exception {
		System.out.println("Filter trades any column Value Greater or Equal 8327 Long");
		Filter valuefilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, 
			new BinaryComparator(Bytes.toBytes(8327l)));
		tradeDao.scanTradesWithFilter(valuefilter);
	}

	public static void scanPrefixFilter(TradeDAOFlat tradeDao) throws Exception {
		System.out.println("Prefix filter GOOG");
		Filter filter = new PrefixFilter(Bytes.toBytes("GOOG"));
		tradeDao.scanTradesWithFilter(filter);
	}

	public static void scanColumnFilter(TradeDAOFlat tradeDao) throws Exception {
		System.out.println("Column Filter");
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
			TradeDAOFlat.volumeCF,
			Bytes.toBytes("11"),
			CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(8327l)));
		filter.setFilterIfMissing(true);
		tradeDao.scanTradesWithFilter(filter);
	}

	public static void scanListFilter(TradeDAOFlat tradeDao) throws Exception {
		System.out.println("List Filter");
		FilterList filterlist = new FilterList(Operator.MUST_PASS_ALL);
		Filter famfilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, 
			new BinaryComparator(Bytes.toBytes("vol")));
		Filter valuefilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, 
			new BinaryComparator(Bytes.toBytes(8327l)));
		filterlist.addFilter(famfilter);
		filterlist.addFilter(valuefilter);
		tradeDao.scanTradesWithFilter(filterlist);
	}

	public static void printTrades(List<Trade> trades) {
		System.out.println("Printing " + trades.size() + " trades.");

		for (Trade trade : trades) {
			System.out.println(trade);
		}
	}
}
