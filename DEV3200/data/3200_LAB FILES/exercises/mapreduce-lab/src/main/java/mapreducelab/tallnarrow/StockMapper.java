package mapreducelab.tallnarrow;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import static mapreducelab.tallnarrow.StockDriver.COLUMN_FAMILY1;
import static mapreducelab.tallnarrow.StockDriver.PRICE_QUALIFIER;

/*
 * 
 tall narrow map input   key, Result row
 GOOG_922337065438554 column=CF1:price, timestamp=1383943828225, value=9996
 GOOG_922337065438700 column=CF1:price, timestamp=1383943828322, value=5005

 tall narrow map output  <Text, LongWritable> 
 GOOG    9996
 GOOG    5005
 */
public class StockMapper extends TableMapper<Text, LongWritable> {

    private final Text reusableText = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result row, Context context)
	    throws IOException, InterruptedException {
	// pull symbol out of key and convert it to unicode
	String[] keyString = Bytes.toString(key.get()).split("_");
	String symbolString = keyString[0];
	reusableText.set(symbolString);

	// TODO 2 finish
	// from the row get the value for CF1:price
	//       COLUMN_FAMILY1,PRICE_QUALIFIER
	byte[] cellValueBytes = null; // TODO row getValue( );
	long price = Bytes.toLong(cellValueBytes);
	// write KeyOut,ValueOut as Text,LongWritable
	// TODO 2 finish
	// context.write( );
    }
}
