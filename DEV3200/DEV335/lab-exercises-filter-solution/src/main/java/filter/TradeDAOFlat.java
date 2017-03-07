package filter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;


public class TradeDAOFlat implements TradeDAO {
    private final HTableInterface table;
    public final static byte[] priceCF = Bytes.toBytes("price");
    public final static byte[] volumeCF = Bytes.toBytes("vol");
    public final static byte[] statsCF = Bytes.toBytes("stats");
    private final static DateFormat rowkeyDateFormat = new SimpleDateFormat("yyyyMMdd");
    private final static DateFormat columnHourFormat = new SimpleDateFormat("HH");
    private final static char delimChar = '_';
    public static final String userdirectory = ".";
    public static final String tablePath = userdirectory + "/trades_flat";

    public TradeDAOFlat(HTableInterface table) throws IOException {
        this.table = table;
    }

    public TradeDAOFlat(Configuration conf) throws IOException {
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
        byte[] hourCol = Bytes.toBytes(columnHourFormat.format(trade.getTime()));

        // Put the price to the price column family
        Put put = new Put(Bytes.toBytes(rowkey));

        // The value to store is (long) price*100
        Float priceNoDecimals = trade.getPrice() * 100f;

        // Store as byte array of long, not float
        byte[] priceNoDecimalsAsLongBytes = Bytes.toBytes(priceNoDecimals.longValue());
        put.add(priceCF, hourCol, trade.getTime(), priceNoDecimalsAsLongBytes);
        put.add(volumeCF, hourCol, trade.getTime(), Bytes.toBytes(trade.getVolume()));

        // Put the volume to the volume column family
        table.put(put);
    }

    private String formRowkey(String symbol, Long time) {
        String timeString = rowkeyDateFormat.format(time);
        String rowkey = symbol + delimChar + timeString;
        System.out.println("DEBUG formRowkey(): formatted rowkey as: " + rowkey);

        return rowkey;
    }

    @Override
    public List<Trade> getTradesByDate(String symbol, Long from, Long to) throws IOException {
        // Create a list to store resulting trades
        List<Trade> trades = new ArrayList<Trade>();

        // Scan all applicable rows for the symbol, between given timestamps
        System.out.println("DEBUG getTradesByDate(): from= " + from + ", to= " + to);
        Scan scan = new Scan(Bytes.toBytes(formRowkey(symbol, from)), Bytes.toBytes(formRowkey(symbol, to)));
        scan.addFamily(priceCF);
        scan.addFamily(volumeCF);
        scan.setMaxVersions();

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            // scanner has one row result per Symbol per day
            createTradesFromResult(trades, result);
        }

        return trades;
    }

    public List<Trade> scanTradesNoFilter() throws IOException {
        Scan scan = mkScan();
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("************************************");
        System.out.println("Scan results for Table without any filters:");

        // Create a list to store resulting trades
        List<Trade> trades = new ArrayList<Trade>();

        for (Result scanResult : scanner) {
            System.out.println("Scan Result");
            Tools.resultMapToString(scanResult);
            createTradesFromResult(trades, scanResult);
        }

        scanner.close();
        return trades;
    }

    public void scanTradesWithFilter(Filter filter) throws IOException {
        Scan scan = mkScan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("************************************");
        System.out.println("Scan results for Table with filters:");

        for (Result scanResult : scanner) {
            System.out.println("Scan Result");
            Tools.resultMapToString(scanResult);
        }

        scanner.close();
    }

    private Scan mkScan() {
        Scan scan = new Scan();
        scan.setMaxVersions();
        return scan;
    }

    private void createTradesFromResult(List<Trade> trades, Result result) {
        String rowkey = Bytes.toString(result.getRow());
        String[] rowkeyTokens = rowkey.split(String.valueOf(delimChar));
        String symbol = rowkeyTokens[0];
        System.out.println(" get Trade data from Scan Result for row key " + rowkey + " Symbol " + symbol);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> priceQualMap = familyMap.get(priceCF);
        NavigableMap<byte[], NavigableMap<Long, byte[]>> volQualMap = familyMap.get(volumeCF);
        Set<byte[]> qualifiers = priceQualMap.keySet();

        // loop through column qualifiers for Family Map
        for (byte[] colQualifier : qualifiers) {
            System.out.println(" get Trade data  from Family Maps for column " + Bytes.toString(colQualifier));
            NavigableMap<Long, byte[]> priceTsMap = priceQualMap.get(colQualifier);
            NavigableMap<Long, byte[]> volTsMap = volQualMap.get(colQualifier);
            Set<Long> tstamps = priceTsMap.keySet();

            // loop through time stamps for Column Maps
            for (Long tstamp : tstamps) {
                // get price and volume for this timestamp from Column maps
                byte[] priceBytes = priceTsMap.get(tstamp);
                byte[] volumeBytes = volTsMap.get(tstamp);
                Float price = Bytes.toLong(priceBytes) / 100f;
                Long volume = Bytes.toLong(volumeBytes);
                System.out.println(" get Trade data from Column Maps for time stamp  " 
                    + tstamp + " , Price " + price + " , Volume " + volume);
                trades.add(new Trade(symbol, price, volume, tstamp));
            }
        }
    }
}
