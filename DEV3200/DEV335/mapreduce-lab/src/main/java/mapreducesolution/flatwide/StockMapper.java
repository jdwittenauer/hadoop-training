package mapreducesolution.flatwide;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class StockMapper extends TableMapper<Text, LongWritable> {
    @Override
    protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {
		String keyString = Bytes.toString(key.get());
		final Text outKeyText = new Text(keyString);

		for (Cell col : row.rawCells()) {
			byte[] cellValueBytes = CellUtil.cloneValue(col);
			long price = 0;

				if (cellValueBytes != null)
					price=Bytes.toLong(cellValueBytes);

			System.out.println(keyString + " price " + price);
			context.write(outKeyText, new LongWritable(price));
		}
    }
}
