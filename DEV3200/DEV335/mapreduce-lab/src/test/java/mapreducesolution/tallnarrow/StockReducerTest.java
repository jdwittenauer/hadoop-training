package mapreducesolution.tallnarrow;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

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

public class StockReducerTest {
    // <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    ReduceDriver<Text, LongWritable, ImmutableBytesWritable, Mutation> reduceDriver;

    @Before
    public void setUp() {
	StockReducer reducer = new StockReducer();
	reduceDriver = ReduceDriver.newReduceDriver(reducer);
	Configuration conf = 	reduceDriver.getConfiguration();
	conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"  + "org.apache.hadoop.hbase.mapreduce.MutationSerialization,"
	  +"org.apache.hadoop.hbase.mapreduce.ResultSerialization,"  +"org.apache.hadoop.hbase.mapreduce.KeyValueSerialization," + "org.apache.hadoop.io.serializer.WritableSerialization");

    }

    // uncomment to run
 //   @Test
    public void testHBaseReduce() throws IOException {


	// Setup Input key 
	String strKey = "GOOG";

	// Setup Input values
	List<LongWritable> list = new ArrayList<LongWritable>();
	long inMin = 30466l;
	long inMax = 60000l;
	list.add(new LongWritable(inMin));
	list.add(new LongWritable(50000l));
	list.add(new LongWritable(inMax));
	list.add(new LongWritable(40000l));
	long inMean = (inMin + 50000l + inMax + 40000l) / 4;

	// Set Input to what mapper would pass
	reduceDriver.withInput(new Text(strKey), list);
	// run the reducer and get its output
	List<Pair<ImmutableBytesWritable, Mutation>> result = reduceDriver.run();

	// extract row key from result and verify
	assertEquals(Bytes.toString(result.get(0).getFirst().get()), strKey);

	// extract values for CF/QUALIFIERs and verify
	Pair<ImmutableBytesWritable, Mutation> putPair = result.get(0);
	Put put = (Put) putPair.getSecond();
	Cell minKeyValue = put.get(StockDriver.COLUMN_FAMILY1, StockDriver.MIN_QUALIFIER).get(0);
	String min = Bytes.toString(CellUtil.cloneValue(minKeyValue));
	System.out.println(" output min " + min);
	String max = Bytes.toString(CellUtil.cloneValue(put.get(StockDriver.COLUMN_FAMILY1, StockDriver.MAX_QUALIFIER)
		.get(0)));
	System.out.println(" output max " + max);
	String mean = Bytes.toString(CellUtil.cloneValue(put.get(StockDriver.COLUMN_FAMILY1, StockDriver.MEAN_QUALIFIER)
		.get(0)));
	System.out.println(" output mean " + mean);
	assertEquals(Float.toString((float) inMin / 100f), min);
	assertEquals(Float.toString((float) inMax / 100f), max);
	assertEquals(Float.toString((float) inMean / 100f), mean);
    }

}