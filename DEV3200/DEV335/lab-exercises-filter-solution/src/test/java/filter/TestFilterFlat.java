package filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFilterFlat {

    static TradeDAOFlat tradeDao = null;

    @BeforeClass
    public static void onlyOnce() throws Exception {
	tradeDao = init();
    }

    private static TradeDAOFlat init() throws Exception {
	List<Trade> testTradeSet = generateDataSet();
	HTableInterface table = MockHTable.create();
	TradeDAOFlat tradeDao = new TradeDAOFlat(table);
	CreateTable.saveData(tradeDao, testTradeSet);
	return tradeDao;

    }

     @Test
    public void testScanWithoutFilters() throws Exception {
	System.out.println(" Test No Filter ");
	FilterTradesFlat.scanWithoutFilters(tradeDao);
    }



    @Test
    public void testSingleColumnValueFilter() throws Exception {
	System.out.println(" Test SingleColumnValueFilter ");
	// Using SingleColumnValueFilter
	FilterTradesFlat.scanColumnFilter(tradeDao);
    }

    @Test
    public void testPrefixFilter() throws Exception {
	System.out.println(" Test PrefixFilter ");
	FilterTradesFlat.scanPrefixFilter(tradeDao);

    }

    @Test
    public void testFilterList() throws Exception {
	System.out.println(" Test Filter List ");
	FilterTradesFlat.scanListFilter(tradeDao);
    }

    public static List<Trade> generateDataSet() {
	List<Trade> trades = new ArrayList<Trade>();
	// String tradeSymbol, Float tradePrice, Long tradeVolume, Long
	// tradeTime
	// AMZN_20131010: 60071 shares at $600.71 at 2013.10.10 11:01:19
	// CSCO_20131009: 50071 shares at $500.71 at 2013.10.09 07:14:39
	// GOOG_20131010: 86724 shares at $867.24 at 2013.10.10 10:36:16
	// GOOG_20131010: 76724 shares at $767.24 at 2013.10.10 10:36:15
	//
	// GOOG: $867.24 7327 shares 2013.10.10 10:36:16
	trades.add(new Trade("GOOG", 867.24f, 7327l, 1381415776l * 1000));
	trades.add(new Trade("GOOG", 767.24f, 8327l, 1381415775l * 1000));
	// AMZN: $600.71 8326 shares 2013.10.10 11:01:19
	trades.add(new Trade("AMZN", 600.71f, 8326l, 1381417279l * 1000));
	// CSCO: $500.71 8326 shares 2013.10.09 07:14:39
	trades.add(new Trade("CSCO", 500.71f, 8326l, 1381317279l * 1000));
	return trades;
    }
}
