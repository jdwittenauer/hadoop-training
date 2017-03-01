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

/*
 * 
 flat wide map input  key,  row
 GOOG_20131024        column=price:15, timestamp=1382655321309, value=9996
 GOOG_20131024        column=price:15, timestamp=1382662214757, value=7059
 GOOG_20131024        column=price:18, timestamp=1382663397116, value=5005

 flat wide map output KeyOut,ValueOut 
 GOOG_20131025   5135
 GOOG_20131025   6155
 GOOG_20131025   5095
 * 
 */
//extends TableMapper KeyOut,ValueOut
public class StockMapper extends TableMapper<Text, LongWritable> {

    // override map (KeyIn rowKey ,ValueIn row, jobContext)
    @Override
    protected void map(ImmutableBytesWritable key, Result row, Context context)
	    throws IOException, InterruptedException {
	// key is in format: AMZN_20131010
	// convert key to unicode String
	String keyString = Bytes.toString(key.get());
	// convert keyString to Text
	final Text outKeyText = new Text(keyString);
	// for all columns in input row
	for (Cell col : row.rawCells()) {
	    // get the value from column in bytes
	    byte[] cellValueBytes = CellUtil.cloneValue(col);
	    // convert value to price in long
	    long price = 0;
		if (cellValueBytes !=null)
			price=Bytes.toLong(cellValueBytes);
	    // write KeyOut,ValueOut as Text,LongWritable
	    System.out.println(keyString + " price " + price);
	    context.write(outKeyText, new LongWritable(price));
	}

    }
}