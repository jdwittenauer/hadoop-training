package filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;


public class TradeDAOTall implements TradeDAO {
    private final HTableInterface table;
    public final static String CF1 = "CF1";
    public final static String PRICE = "price";
    public final static String VOL = "vol";
    public final static byte[] baseCF = Bytes.toBytes(CF1);
    public final static byte[] priceCol = Bytes.toBytes(PRICE);
    public final static byte[] volumeCol = Bytes.toBytes(VOL);
    public final static char delimChar = '_';
    public static final String userdirectory = ".";
    public static final String tablePath = userdirectory + "/trades_tall";

    public TradeDAOTall(HTableInterface table) throws IOException {
        this.table = table;
    }

    public TradeDAOTall(Configuration conf) throws IOException {
        table = new HTable(conf, tablePath);
    }

    @Override
    public void close() throws IOException {
        table.close();
    }

    @Override
    public void store(Trade trade) throws IOException {
        String rowkey = formRowkey(trade.getSymbol(), trade.getTime());
        System.out.println("Put trade: " + trade);
        Put put = new Put(Bytes.toBytes(rowkey));
        Float priceNoDecimals = trade.getPrice() * 100f;
        System.out.println("Put key: " + rowkey + " family: " + CF1
            + " columns: " + PRICE + " " + +priceNoDecimals.longValue()
            + " " + VOL + " " + trade.getVolume());
        put.add(baseCF, priceCol, Bytes.toBytes(priceNoDecimals.longValue()));
        put.add(baseCF, volumeCol, Bytes.toBytes(trade.getVolume()));
        table.put(put);
    }

    public String formRowkey(String symbol, Long time) {
        String timeString = String.format("%d", (Long.MAX_VALUE - time));
        String rowkey = symbol + delimChar + timeString;

        return rowkey;
    }

    public String formRowkey(String symbol, String inString) throws Exception {
        long timeEpoch = TimeUtil.convertStringToEpoch(inString);
        long reverseTime = Long.MAX_VALUE - timeEpoch;
        String timeString = String.format("%d", reverseTime);
        String rowkey = symbol + delimChar + timeString;
        System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey);

        return rowkey;
    }

    @Override
    public List<Trade> getTradesByDate(String symbol, Long from, Long to) throws IOException {
        // create a list to store resulting trades
        List<Trade> trades = new ArrayList<Trade>();

        // scan all applicable rows for the symbol, between given timestamps
        Scan scan = new Scan(Bytes.toBytes(formRowkey(symbol, to)), 
            Bytes.toBytes(formRowkey(symbol, from)));
        scan.addFamily(baseCF);
        ResultScanner scanner = table.getScanner(scan);

        // iterate through the scanner, add scan result to list of Trades
        for (Result result : scanner) {
            createTradeFromResult(trades, result);
        }

        return trades;
    }

    public void createTradeFromResult(List<Trade> trades, Result result) {
		// extract the symbol & timestamp from the result's rowkey
        String rowkey = Bytes.toString(result.getRow());

        // tokenize rowkey
        String[] rowkeyTokens = rowkey.split(String.valueOf(delimChar));

        // reconstitute a valid timestamp from the rowkey digits
        Long time = Long.MAX_VALUE - Long.parseLong(rowkeyTokens[1]);
        float price = 0;
        long volume = 0;

        if (result.containsColumn(baseCF, priceCol)) {
            price = (float) (Bytes.toLong(result.getValue(baseCF, priceCol))) / 100f;
        }

        if (result.containsColumn(baseCF, volumeCol)) {
            volume = Bytes.toLong(result.getValue(baseCF, volumeCol));
        }

        // add the new trade to the list of trades
        trades.add(new Trade(rowkeyTokens[0], price, volume, time));
    }

    public Scan mkScan() {
        Scan scan = new Scan();
        scan.setMaxVersions();

        return scan;
    }

    public List<Trade> scanTradesNoFilter() throws IOException {
        Scan scan = mkScan();
        ResultScanner scanner = table.getScanner(scan);

        // create a list to store resulting trades
        List<Trade> trades = new ArrayList<Trade>();

        for (Result scanResult : scanner) {
            Tools.resultMapToString(scanResult);
            createTradeFromResult(trades, scanResult);
        }

        scanner.close();
        return trades;
    }

    public List<Trade> scanTradesWithFilter(Filter filter) throws IOException {
        Scan scan = mkScan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("************************************");

        // for each scanResult call resultToString
        List<Trade> trades = new ArrayList<Trade>();

        for (Result scanResult : scanner) {
            System.out.println(Tools.resultMapToString(scanResult));
            createTradeFromResult(trades, scanResult);
        }

        scanner.close();
        return trades;
    }

    public List<Trade> getTrades() throws IOException {
        Scan scan = new Scan();
        scan.setMaxVersions();
        ResultScanner scanner = table.getScanner(scan);

        // Create a list to store resulting trades
        List<Trade> trades = new ArrayList<Trade>();

        for (Result scanResult : scanner) {
            Tools.resultMapToString(scanResult);
            createTradeFromResult(trades, scanResult);
        }

        scanner.close();
        return trades;
    }
}
