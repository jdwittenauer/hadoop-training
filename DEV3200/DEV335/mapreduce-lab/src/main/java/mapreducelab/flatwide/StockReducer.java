package mapreducelab.flatwide;

// TODO 4 : add output column for max
// TODO 4 : add output column for mean

import static mapreducelab.flatwide.StockDriver.COLUMN_FAMILY_STATS;
import static mapreducelab.flatwide.StockDriver.COL_QUALIFIER_MIN;

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

public class StockReducer extends
	TableReducer<Text, LongWritable, ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> prices, Context context)
	    throws IOException, InterruptedException {
	// TODO Exercise 4: calculate max and mean
	int count = 0;
	long min = Long.MAX_VALUE;
	long max = Long.MIN_VALUE;
	long temp = 0L;

	// calculate min, max, and mean
	// TODO Exercise 3: calcuate min
	// iterate through the input prices
	// temp = price.get();
	if (temp < min)
	    min = temp;

	// emit mean, min, and max stats
	byte[] keyB = Bytes.toBytes(key.toString());
	Put put = new Put(keyB);
	// TODO Exercise 4: add put statement for max value
	put.add(COLUMN_FAMILY_STATS, COL_QUALIFIER_MIN,
		Bytes.toBytes(Float.toString((float) min / 100f)));
	// TODO: Exercise 4: add put statement for mean value
	// TODO: Exercise 4: add put statement for min value
	// write KEYOUT VALUEOUT
	context.write(new ImmutableBytesWritable(keyB), put);
    }
}
