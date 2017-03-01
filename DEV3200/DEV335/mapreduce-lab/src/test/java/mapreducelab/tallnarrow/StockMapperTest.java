package mapreducelab.tallnarrow;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreducelab.KeyValueTestUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

/*
 * 
 tall narrow map input   key, Result row
 GOOG_922337065438554 column=CF1:price, timestamp=1383943828225, value=9996
 GOOG_922337065438700 column=CF1:price, timestamp=1383943828322, value=5005

 tall narrow map output  <Text, LongWritable> 
 GOOG    9996
 GOOG    5005
 */
public class StockMapperTest {
	// <KeyIn,ValueIn,KeyOut,ValueOut> rowkey, row Result, outKey, outValue
	MapDriver<ImmutableBytesWritable, Result, Text, LongWritable> mapDriver;

	@Before
	public void setUp() {
		StockMapper mapper = new StockMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		Configuration conf = 	mapDriver.getConfiguration();
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"  + "org.apache.hadoop.hbase.mapreduce.MutationSerialization,"
		  +"org.apache.hadoop.hbase.mapreduce.ResultSerialization,"  +"org.apache.hadoop.hbase.mapreduce.KeyValueSerialization," + "org.apache.hadoop.io.serializer.WritableSerialization");
	}

	// uncomment to run
  //  @Test
	public void testHBaseInsert() throws IOException {


		String inKey = "AMZN_20131021";
		String outKey = "AMZN";
		long price = 10l;
		LongWritable priceOut = new LongWritable(price);

		// Setup Test input (key, scan Result row list)
		ImmutableBytesWritable key = new ImmutableBytesWritable(
				Bytes.toBytes(inKey));
		List<Cell> columnValuesList = new ArrayList<Cell>();
		// row format= "colfam" "price", colqual hour, timestamp, value price
		// create KeyValue with key, "colfam", "colqual", timestamp , value
		Cell k1 = KeyValueTestUtil.create(inKey, "CF1", StockDriver.PRICE,
				System.currentTimeMillis() / 1000, price);

		columnValuesList.add(k1);

		// Setup Test input: like a scan row result
		Result result = Result.create(columnValuesList);

		// Set Input
		mapDriver.withInput(key, result);
		// run the reducer and get its output
		List<Pair<Text, LongWritable>> mapResult = mapDriver.run();

		Text expectedOutput = new Text(outKey);
		// extract value for CF/QUALIFIER and verify
		// output is key price
		Pair<Text, LongWritable> pair = mapResult.get(0);
		Text text = pair.getFirst();
		// extract key from result and verify
		assertEquals(expectedOutput, text);
		System.out.println(" output text " + text);
		LongWritable longW = pair.getSecond();
		System.out.println(" output price " + longW);
		assertEquals(priceOut, longW);

	}

}