package schemadesign;

import static org.junit.Assert.assertEquals;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;

public class MyHBaseTestTall {

	private final static DateFormat dateArgFormat = new SimpleDateFormat(
			"yyyyMMdd");

	@Before
	public void setup() throws Exception {

	}

	@Test
	public void testPutScan() throws Exception {
		List<Trade> testTradeSet = generateDataSet();
		HTableInterface table = MockHTable.create();
		TradeDAO tradeDao = new TradeDAOTall(table);
		CreateTable.saveData(tradeDao, testTradeSet);
		String symbol = "GOOG";
		Long startDate = dateArgFormat.parse("20131010", new ParsePosition(0))
				.getTime();
		Long stopDate = dateArgFormat.parse("20131011", new ParsePosition(0))
				.getTime();
		List<Trade> retrievedTradeSet = tradeDao.getTradesByDate(symbol,
				startDate, stopDate);
		// Print the results
		LookupTrades.printTrades(retrievedTradeSet);
		assertEquals(retrievedTradeSet.size(), 1);
		Trade trade = retrievedTradeSet.get(0);
		assertEquals("GOOG", trade.getSymbol());

		symbol = "AMZN";
		startDate = dateArgFormat.parse("20131010", new ParsePosition(0))
				.getTime();
		stopDate = dateArgFormat.parse("20131011", new ParsePosition(0))
				.getTime();
		retrievedTradeSet = tradeDao.getTradesByDate(symbol, startDate,
				stopDate);
		// Print the results
		LookupTrades.printTrades(retrievedTradeSet);
		assertEquals(retrievedTradeSet.size(), 1);
		trade = retrievedTradeSet.get(0);
		assertEquals("AMZN", trade.getSymbol());

		symbol = "AMZN";
		startDate = dateArgFormat.parse("20131009", new ParsePosition(0))
				.getTime();
		stopDate = dateArgFormat.parse("20131009", new ParsePosition(0))
				.getTime();
		retrievedTradeSet = tradeDao.getTradesByDate(symbol, startDate,
				stopDate);
		assertEquals(0, retrievedTradeSet.size());

		symbol = "CSCO";
		startDate = dateArgFormat.parse("20131009", new ParsePosition(0))
				.getTime();
		stopDate = dateArgFormat.parse("20131010", new ParsePosition(0))
				.getTime();
		retrievedTradeSet = tradeDao.getTradesByDate(symbol, startDate,
				stopDate);
		// Print the results
		LookupTrades.printTrades(retrievedTradeSet);
		assertEquals(retrievedTradeSet.size(), 1);
		trade = retrievedTradeSet.get(0);
		assertEquals("CSCO", trade.getSymbol());
		assertEquals(Float.valueOf(500.71f), trade.getPrice());

	}

	public static List<Trade> generateDataSet() {
		List<Trade> trades = new ArrayList<Trade>();
		// String tradeSymbol, Float tradePrice, Long tradeVolume, Long
		// tradeTime
		// GOOG: $867.24 7327 shares 2013.10.10 10:36:16
		trades.add(new Trade("GOOG", 867.24f, 7327l, 1381415776l * 1000));
		// AMZN: $600.71 8326 shares 2013.10.10 11:01:19
		trades.add(new Trade("AMZN", 600.71f, 8326l, 1381417279l * 1000));
		// CSCO: $500.71 8326 shares 2013.10.09 07:14:39
		trades.add(new Trade("CSCO", 500.71f, 8326l, 1381317279l * 1000));
		return trades;
	}
}
