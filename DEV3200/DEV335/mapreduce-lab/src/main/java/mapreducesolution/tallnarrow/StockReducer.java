package mapreducesolution.tallnarrow;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/*
tall narrow   reduce input   Text key, Iterable<LongWritable> prices
GOOG    9996
GOOG    5005

tall narrow  reduce output key ImmutableBytesWritable, put 
 GOOG                 column=CF1:count, timestamp=1385138594695, value=37
 GOOG                 column=CF1:max, timestamp=1385138594695, value=99.96
 GOOG                 column=CF1:mean, timestamp=1385138594695, value=75.59
 GOOG                 column=CF1:min, timestamp=1385138594695, value=50.05
*/

public class StockReducer extends
	TableReducer<Text, LongWritable, ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> prices, Context context)
	    throws IOException, InterruptedException {
	int count = 0;
	long min = Long.MAX_VALUE;
	long sum = 0L;
	long max = Long.MIN_VALUE;
	long temp = 0L;

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
	float mean = sum / count;

	// emit mean, min, and max stats
	byte[] keyB = Bytes.toBytes(key.toString());
	Put put = new Put(keyB);
	put.add(StockDriver.COLUMN_FAMILY1, StockDriver.MEAN_QUALIFIER,
		Bytes.toBytes(Float.toString((float) mean / 100)));
	put.add(StockDriver.COLUMN_FAMILY1, StockDriver.MIN_QUALIFIER,
		Bytes.toBytes(Float.toString((float) min / 100)));
	put.add(StockDriver.COLUMN_FAMILY1, StockDriver.MAX_QUALIFIER,
		Bytes.toBytes(Float.toString((float) max / 100)));
	put.add(StockDriver.COLUMN_FAMILY1, Bytes.toBytes("count"),
		Bytes.toBytes(Integer.toString(count)));
	context.write(new ImmutableBytesWritable(keyB), put);
    }
}
