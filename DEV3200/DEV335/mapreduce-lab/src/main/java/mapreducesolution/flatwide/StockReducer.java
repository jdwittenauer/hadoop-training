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


public class StockReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> prices, Context context) throws IOException, InterruptedException {
		long sum = 0l;
		int count = 0;
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		long temp = 0l;

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
		byte[] keyB = Bytes.toBytes(key.toString());

		Put put = new Put(keyB);
		put.add(COLUMN_FAMILY_STATS, COL_QUALIFIER_MEAN, Bytes.toBytes(Float.toString((float) mean / 100f)));
		put.add(COLUMN_FAMILY_STATS, COL_QUALIFIER_MIN, Bytes.toBytes(Float.toString((float) min / 100f)));
		put.add(COLUMN_FAMILY_STATS, COL_QUALIFIER_MAX, Bytes.toBytes(Float.toString((float) max / 100f)));
		put.add(COLUMN_FAMILY_STATS, Bytes.toBytes("count"), Bytes.toBytes(Integer.toString(count)));

		context.write(new ImmutableBytesWritable(keyB), put);
    }
}
