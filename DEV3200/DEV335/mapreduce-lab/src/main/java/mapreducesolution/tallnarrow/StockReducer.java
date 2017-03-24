package mapreducesolution.tallnarrow;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class StockReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> prices, Context context) throws IOException, InterruptedException {
		int count = 0;
		long min = Long.MAX_VALUE;
		long sum = 0L;
		long max = Long.MIN_VALUE;
		long temp = 0L;

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

		byte[] keyB = Bytes.toBytes(key.toString());
		Put put = new Put(keyB);
		put.add(StockDriver.COLUMN_FAMILY1, StockDriver.MEAN_QUALIFIER, Bytes.toBytes(Float.toString((float) mean / 100)));
		put.add(StockDriver.COLUMN_FAMILY1, StockDriver.MIN_QUALIFIER, Bytes.toBytes(Float.toString((float) min / 100)));
		put.add(StockDriver.COLUMN_FAMILY1, StockDriver.MAX_QUALIFIER, Bytes.toBytes(Float.toString((float) max / 100)));
		put.add(StockDriver.COLUMN_FAMILY1, Bytes.toBytes("count"), Bytes.toBytes(Integer.toString(count)));

		context.write(new ImmutableBytesWritable(keyB), put);
    }
}
