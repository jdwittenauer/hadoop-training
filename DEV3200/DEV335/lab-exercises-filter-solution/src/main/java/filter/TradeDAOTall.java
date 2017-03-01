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
    // public static final String userdirectory = System.getProperty("user.home");
    public static final String tablePath = userdirectory + "/trades_tall";

    public TradeDAOTall(HTableInterface table) throws IOException {
        this.table = table;
    }

    /**
     * constructs a TradeDAO using a tall-narrow table schema. This
     * implementation takes a pathToTable for the data table.
     *
     * @param conf the HBase configuration
     * @param pathToTable the path to the table, stated from the root of the
     * Hadoop filesystem. pass null to use a default table location.
     * @throws IOException
     */
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
        // The value to store is (long) price*100
        Float priceNoDecimals = trade.getPrice() * 100f;
        // Store as byte array of long, not float
        System.out.println("Put key: " + rowkey + " family: " + CF1
                + " columns: " + PRICE + " " + +priceNoDecimals.longValue()
                + " " + VOL + " " + trade.getVolume());
        put.add(baseCF, priceCol, Bytes.toBytes(priceNoDecimals.longValue()));
        put.add(baseCF, volumeCol, Bytes.toBytes(trade.getVolume()));

        table.put(put);
    }

    /**
     * generates a rowkey for tall table implementation. rowkeys descend as the
     * timestamp parameter ascends. rowkey format = SYMBOL_REVERSETIMESTAMP
     * Example: GOOG_9223370654476953800
     *
     * @param symbol
     * @param time
     * @return
     */
    public String formRowkey(String symbol, Long time) {
        // Construct the rowkey
        String timeString = String.format("%d", (Long.MAX_VALUE - time));
        String rowkey = symbol + delimChar + timeString;
		// System.out.println("DEBUG formRowkey(): formatted rowkey as: " +
        // rowkey);
        return rowkey;
    }

    public String formRowkey(String symbol, String inString) throws Exception {
        // Construct the rowkey
        long timeEpoch = TimeUtil.convertStringToEpoch(inString);
        long reverseTime = Long.MAX_VALUE - timeEpoch;
        String timeString = String.format("%d", reverseTime);
        String rowkey = symbol + delimChar + timeString;
        System.out
                .println("DEBUG formRowkey(): formatted rowkey as: " + rowkey); // DEBUG
        return rowkey;
    }

    /**
     * retrieves all results for a given symbol, between two timestamps
     */
    @Override
    public List<Trade> getTradesByDate(String symbol, Long from, Long to)
            throws IOException {
        // Create a list to store resulting trades
        List<Trade> trades = new ArrayList<Trade>();
        // Scan all applicable rows for the symbol, between given timestamps
        Scan scan = new Scan(Bytes.toBytes(formRowkey(symbol, to)),
                Bytes.toBytes(formRowkey(symbol, from)));
        scan.addFamily(baseCF);

        ResultScanner scanner = table.getScanner(scan);

        // Iterate through the scanner, add scan result to list of Trades.
        for (Result result : scanner) {
            createTradeFromResult(trades, result);
        }
        return trades;
    }

    public void createTradeFromResult(List<Trade> trades, Result result) {
		// Extract the symbol & timestamp from the result's rowkey
        // rowkey format is SYMBOL_TIMESTAMP
        String rowkey = Bytes.toString(result.getRow());

        // tokenize rowkey
        String[] rowkeyTokens = rowkey.split(String.valueOf(delimChar));
        // reconstitute a valid timestamp from the rowkey digits
        Long time = Long.MAX_VALUE - Long.parseLong(rowkeyTokens[1]);
		// Extract price & volume from the result
        // (Price*100) is stored as long byte-array.
        // Divide by 100 to extract to a Float.
        float price = 0;
        long volume = 0;
        if (result.containsColumn(baseCF, priceCol)) {
            price = (float) (Bytes.toLong(result.getValue(baseCF, priceCol))) / 100f;
        }
        if (result.containsColumn(baseCF, volumeCol)) {
            volume = Bytes.toLong(result.getValue(baseCF, volumeCol));
        }

        // Add the new trade to the list of trades
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

        // Create a list to store resulting trades
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
		// TODO 2 finish code
        // set input filter on scan
        scan.setFilter(filter);
        // call table getScanner()
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("************************************");

        // for each scanResult call resultToString
        List<Trade> trades = new ArrayList<Trade>();
        for (Result scanResult : scanner) {
            //	System.out.println("Scan Result " + scanResult);
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
