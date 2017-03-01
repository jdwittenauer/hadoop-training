package mapreducesolution.flatwide;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreducelab.KeyValueTestUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
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
    public void testHBaseMapper() throws IOException {
	// Setup Input key 
	String strKey = "AMZN_20131021";

	// Setup Test input (key, scan Result row list)
	ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(strKey));
	List<Cell> columnValuesList = new ArrayList<Cell>();
	// Result row = "colfam" "price", colqual hour, timestamp, value price
	// create KeyValue with key, "colfam", "colqual", timestamp , value
	KeyValue k1 = KeyValueTestUtil.create(strKey, StockDriver.PRICE, "00",
		System.currentTimeMillis() / 1000, 10l);
	KeyValue k2 = KeyValueTestUtil.create(strKey, StockDriver.PRICE, "00", 2l, 12l);
	KeyValue k3 = KeyValueTestUtil.create(strKey, StockDriver.PRICE, "15",
		System.currentTimeMillis() / 1000, 15l);
	columnValuesList.add(k1);
	columnValuesList.add(k2);
	columnValuesList.add(k3);
	// Setup Test input: like a scan row result
	Result result = Result.create(columnValuesList);


	// Set Input
	mapDriver.withInput(key, result);
	// run the reducer and get its output
	List<Pair<Text, LongWritable>> mapResult = mapDriver.run();

	Text expectedOutput = new Text(strKey);
	// extract value for CF/QUALIFIER and verify
	// output is key price
	for (Pair<Text, LongWritable> pair : mapResult) {
	    Text text = pair.getFirst();
	    // extract key from result and verify
	    assertEquals(expectedOutput, text);
	    System.out.println(" output text " + text);
	    LongWritable longW = pair.getSecond();
	    System.out.println(" output price " + longW);
	}
    }

}