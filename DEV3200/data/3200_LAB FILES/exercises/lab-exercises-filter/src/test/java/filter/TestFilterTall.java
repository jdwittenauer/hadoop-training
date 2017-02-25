package filter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;



public class TestFilterTall {

	static TradeDAOTall tradeDao = null;

	@Before
	public void onlyOnce() throws Exception {
		tradeDao = init();
	}

	private TradeDAOTall init() throws Exception {
		List<Trade> testTradeSet = generateDataSet();
		HTableInterface table = MockHTable.create();
		TradeDAOTall tradeDao = new TradeDAOTall(table);
		CreateTable.saveData(tradeDao, testTradeSet);
		return tradeDao;

	}

	// TODO 1 uncomment this test and run it
	// @Test
	public void testScanWithoutFilters() throws Exception {
		System.out.println(" Test No Filter ");
		List<Trade> trades=FilterTradesTall.scanWithoutFilters(tradeDao);
		assertEquals(trades.size(), 4);
	}

	// TODO 2 uncomment this test
	// finish code
	// run test until green
	//@Test
	public void testRowFilter1() throws Exception {
		System.out.println(" Test Row Filter ");
		// row key = symbol + delim + reverse time (long)
		String filterString = "GOOG";
		System.out.println("******filter row keys containing string "
				+ filterString);
		// TODO 2 finish code in FilterTradesTall.scanRowFilter
		List<Trade> trades=FilterTradesTall.scanRowFilter(tradeDao, filterString);
		assertEquals(trades.size(), 2);
	}

	// TODO 2b uncomment and run this test
   // @Test
	public void testRowFilter2() throws Exception {
		System.out.println(" Test Row Filter 2");
		// get time in epoch long
		long timeEpoch = TimeUtil.convertStringToEpoch("10/10/2013 00:00:00");
		// calculate reverse time
		long reverseTime = Long.MAX_VALUE - timeEpoch;
		System.out.println("10/10/2013 00:00:00 time epoch long  " + timeEpoch
				+ " reverse epoch long " + reverseTime);
		// get time substring for date
		String filterString = Long.toString(reverseTime).substring(0, 8);
		System.out.println("******filter row keys containing string "
				+ filterString);
		List<Trade> trades=FilterTradesTall.scanRowFilter(tradeDao, filterString);
		assertEquals(trades.size(), 4);
		Trade trade = trades.get(0);
		assertTrue( trade.getTime()>=timeEpoch);

		// filter keys containing google + delim + substring for reverse date
		// time
		filterString = "GOOG" + TradeDAOTall.delimChar + filterString;
		System.out.println("******filter row keys containing string "
				+ filterString);
		trades=FilterTradesTall.scanRowFilter(tradeDao, filterString);
		assertEquals(trades.size(), 2);
		
		// filter keys containing google + delim + reverse date time
		filterString = tradeDao.formRowkey("GOOG", "10/10/2013 10:36:16");
		System.out.println("******filter row keys containing string "
				+ filterString);
		trades = FilterTradesTall.scanRowFilter(tradeDao, filterString);
		trade = trades.get(0);
		assertEquals(trades.size(), 1);
		System.out.println(trade.getTime());
	}

	// TODO 2c uncomment and run this test
	// @Test
	public void testRowFilter3() throws Exception {
		System.out.println(" Test Row Filter 2 ");
		List<Trade> trades=FilterTradesTall.scanRowFilter(tradeDao);
		assertEquals(trades.size(), 4);
	}

	// TODO 3 uncomment this test
	// finish code
	// run test until green
	// @Test
	public void testPrefixFilter() throws Exception {
		System.out.println(" Test PrefixFilter ");
		// TODO 3 finish code in FilterTradesTall.scanPrefixFilter
		List<Trade> trades=FilterTradesTall.scanPrefixFilter(tradeDao);
		assertEquals(trades.size(), 2);
	}

	// TODO 4 uncomment this test
	// finish code
	// run test until green
	//@Test
	public void testFilterList() throws Exception {
		System.out.println(" Test Filter List ");
		// TODO 4 finish code in scanListFilter
		List<Trade> trades=FilterTradesTall.scanListFilter(tradeDao);
		Trade trade = trades.get(0);
		long volume = 8000l;
		assertTrue( trade.getVolume()>=volume);
	}



	public static List<Trade> generateDataSet() throws Exception {
		List<Trade> trades = new ArrayList<Trade>();
		// create trades: String tradeSymbol,
		// Float tradePrice, Long tradeVolume, String tradeTime
		// GOOG_20131010: 86724 shares at $867.24 at 2013.10.10 10:36:16
		trades.add(new Trade("GOOG", 867.24f, 7327l, "10/10/2013 10:36:16"));
		// GOOG_20131010: 76724 shares at $767.24 at 2013.10.10 10:36:15
		trades.add(new Trade("GOOG", 767.24f, 8327l, "10/10/2013 10:36:15"));
		// AMZN: $600.71 price 8326 volume 2013.10.10 11:01:19 time
		trades.add(new Trade("AMZN", 600.71f, 6007l, "10/10/2013 11:01:19"));
		// CSCO: $500.71 price 8326 volume 2013.10.09 07:14:39 time
		trades.add(new Trade("CSCO", 500.71f, 8326l, "10/10/2013 07:14:39"));
		return trades;
	}
}
