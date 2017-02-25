package schemadesign;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TradeDAOTall implements TradeDAO {

    private final HTableInterface table;
    private final static byte[] baseCF = Bytes.toBytes("CF1");
    private final static byte[] priceCol = Bytes.toBytes("price");
    private final static byte[] volumeCol = Bytes.toBytes("vol");

    private final static char delimChar = '_';
    public static final String userdirectory = ".";
    //   public static final String userdirectory = System.getProperty("user.home");
    public static String tablePath = userdirectory + "/trades_tall";

    public TradeDAOTall(HTableInterface table) throws IOException {
        this.table = table;
    }

    public TradeDAOTall(Configuration conf) throws IOException {
        table = new HTable(conf, tablePath);
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
    public TradeDAOTall(Configuration conf, String pathToTable) throws IOException {
        if (pathToTable != null) {
            tablePath = pathToTable;
        }
        table = new HTable(conf, tablePath);
    }

    public void close() throws IOException {
        table.close();
    }

    public void store(Trade trade) throws IOException {
        System.out.println("Putting trade: " + trade);
        String rowkey = formRowkey(trade.getSymbol(), trade.getTime());

        Put put = new Put(Bytes.toBytes(rowkey));
        // The value to store is (long) price*100
        Float priceNoDecimals = trade.getPrice() * 100f;
        // Store as byte array of long, not float
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
    private String formRowkey(String symbol, Long time) {
	// TODO 3 task 1: Construct the rowkey
        // use the format SYMBOL_REVERSETIMESTAMP
        // Use a reverse timestamp in the rowkey by subtracting (Long.MAX_VALUE
        // - time)
        // System.out.println("DEBUG formRowkey(): formatted rowkey as: " +
        // rowkey); // DEBUG
        return null;// rowkey;
    }

    /**
     * retrieves all results for a given symbol, between two timestamps
     */
    public List<Trade> getTradesByDate(String symbol, Long from, Long to)
            throws IOException {
	// TODO 3 task 2: Implement getTradesByDate()

        // Create a list to store resulting trades
        List<Trade> trades = new ArrayList<Trade>();

	// Hint: Create a scan instance to scan
        // all applicable rows for the symbol, between given timestamps
        Scan scan = null; // TODO finish
        ResultScanner scanner = null; // TODO call table.getScanner(scan);

	// Hint: Iterate through the resultScanner, and put results in our list
        // of Trades.
        // Populate these: Date tradeDate, String tradeSymbol, Float tradePrice,
        // Long tradeVolume
        for (Result result : scanner) {
	    // TODO
            // Extract the symbol & timestamp from the result's rowkey
            // rowkey format is SYMBOL_REVERSETIMESTAMP
            // reconstitute a valid timestamp from the rowkey's reverse
            // timestamp
            // Extract price & volume from the result
            // create Trade with Constructor Trade(String tradeSymbol, Float
            // tradePrice, Long tradeVolume, Long tradeTime)
            // Add the new trade to the list of trades
        }
        return trades;
    }

}
