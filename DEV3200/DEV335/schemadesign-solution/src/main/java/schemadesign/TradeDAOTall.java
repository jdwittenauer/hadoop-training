package schemadesign;

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
import org.apache.hadoop.hbase.util.Bytes;

public class TradeDAOTall implements TradeDAO {

    private final HTableInterface table;
    private final static byte[] baseCF = Bytes.toBytes("CF1");
    private final static byte[] priceCol = Bytes.toBytes("price");
    private final static byte[] volumeCol = Bytes.toBytes("vol");

    private final static char delimChar = '_';

    public static final String userdirectory = ".";
    public static final String tablePath = userdirectory + "/trades_tall";
 // public static final String tablePath =  "trades_tall";
    public TradeDAOTall(HTableInterface table) throws IOException {
	this.table = table;
    }

    /**
     * constructs a TradeDAO using a tall-narrow table schema. This
     * implementation takes a pathToTable for the data table.
     * 
     * @param conf
     *            the HBase configuration
     * @param pathToTable
     *            the path to the table, stated from the root of the Hadoop
     *            filesystem. pass null to use a default table location.
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
	// Construct the rowkey
	String timeString = String.format("%d", (Long.MAX_VALUE - time));
	String rowkey = symbol + delimChar + timeString;
	System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey); // DEBUG

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

	// Iterate through the scanner, add scan result to  list of Trades.
	for (Result result : scanner) {
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
	    Float price = (float) (Bytes.toLong(result.getValue(baseCF, priceCol))) / 100f;
	    Long volume = Bytes.toLong(result.getValue(baseCF, volumeCol));

	    // Add the new trade to the list of trades
	    trades.add(new Trade(rowkeyTokens[0], price, volume, time));
	}
	return trades;
    }

}
