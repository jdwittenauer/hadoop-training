package mapreducesolution.tallnarrow;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


public class StockMapper extends TableMapper<Text, LongWritable> {
    private static final Logger log = Logger.getLogger(StockMapper.class);
    private final Text reusableText = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result row, Context context)  throws IOException, InterruptedException {
        // pull symbol out of key and convert it to unicode
        log.info("map Result  " + row + "...");
        System.out.println("map Result  " + row + "...");
        String[] keyString = Bytes.toString(key.get()).split("_");
        String symbolString = keyString[0];
        System.out.println("Stock Symbol " + symbolString + " ...");
        System.err.append("Stock Symbol " + symbolString + " ...");
        reusableText.set(symbolString);

        // from the row get the value for CF1:price
        byte[] cellValueBytes = row.getValue(StockDriver.COLUMN_FAMILY1, StockDriver.PRICE_QUALIFIER);
        long price = 0;

        if (cellValueBytes != null)
            price=Bytes.toLong(cellValueBytes);

        System.out.println("Price  " + price + " ...");
        System.err.append("Price " + symbolString + " ...");
        
        context.write(reusableText, new LongWritable(price));
    }
}
