package mapreducesolution.flatwide;

import static mapreducesolution.flatwide.StockDriver.COL_QUALIFIER_MEAN;
import static mapreducesolution.flatwide.StockDriver.COL_QUALIFIER_MIN;
import static mapreducesolution.flatwide.StockDriver.COL_QUALIFIER_MAX;
import static mapreducesolution.flatwide.StockDriver.COLUMN_FAMILY_STATS;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * 
 flat wide reduce input key , prices
 GOOG_20131022   5005 , 6155, 9996, 


 flat wide  reduce output key  put 
 GOOG_20131022        column=stats:count, timestamp=1385137969400, value=37
 GOOG_20131022        column=stats:max, timestamp=1385137969400, value=99.96
 GOOG_20131022        column=stats:mean, timestamp=1385137969400, value=75.59
 GOOG_20131022        column=stats:min, timestamp=1385137969400, value=50.05
 */
//  extends  TableReducer<KEYIN, VALUEIN, KEYOUT>
public class StockReducer extends
	TableReducer<Text, LongWritable, ImmutableBytesWritable> {

    // reduce input from map= KEYIN, VALUEIN
    @Override
    protected void reduce(Text key, Iterable<LongWritable> prices, Context context)
	    throws IOException, InterruptedException {
	long sum = 0l;
	int count = 0;
	long min = Long.MAX_VALUE;
	long max = Long.MIN_VALUE;
	long temp = 0l;

	// calculate min, max, and mean
	for (LongWritable price : prices) {
	    temp = price.get();
	    if (temp > max)
		max = temp;
	    if (temp < min)
		min = temp;
	    sum += temp;
	    count++;
	}
	long mean = sum / count;

	// emit mean, min, and max stats
	byte[] keyB = Bytes.toBytes(key.toString());
	Put put = new Put(keyB);
	put.add(COLUMN_FAMILY_STATS, COL_QUALIFIER_MEAN,
		Bytes.toBytes(Float.toString((float) mean / 100f)));
	put.add(COLUMN_FAMILY_STATS, COL_QUALIFIER_MIN,
		Bytes.toBytes(Float.toString((float) min / 100f)));
	put.add(COLUMN_FAMILY_STATS, COL_QUALIFIER_MAX,
		Bytes.toBytes(Float.toString((float) max / 100f)));
	put.add(COLUMN_FAMILY_STATS, Bytes.toBytes("count"),
		Bytes.toBytes(Integer.toString(count)));
	// write KEYOUT VALUEOUT
	context.write(new ImmutableBytesWritable(keyB), put);
    }
}
