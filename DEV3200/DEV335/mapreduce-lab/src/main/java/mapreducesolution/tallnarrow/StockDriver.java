package mapreducesolution.tallnarrow;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class StockDriver extends Configured implements Tool {
    public static final String userdirectory = ".";
    public static final String TABLE_NAME = userdirectory + "/trades_tall";
    protected final static byte[] COLUMN_FAMILY1 = Bytes.toBytes("CF1");
    protected final static String PRICE = "price";
    protected final static byte[] PRICE_QUALIFIER = Bytes.toBytes(PRICE);
    protected final static String MEAN = "mean";
    protected final static String MAX = "max";
    protected final static String MIN = "min";
    protected final static byte[] MEAN_QUALIFIER = Bytes.toBytes(MEAN);
    protected final static byte[] MAX_QUALIFIER = Bytes.toBytes(MAX);
    protected final static byte[] MIN_QUALIFIER = Bytes.toBytes(MIN);
    private static final Logger log = Logger.getLogger(StockDriver.class);

    public int run(String[] args) throws Exception {
		// setup the Job
		Job job = Job.getInstance(getConf(), getClass().getSimpleName());
		job.setJarByClass(getClass());
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		// instantiate and initialize the scan object
		Scan scan = new Scan();
		scan.addFamily(COLUMN_FAMILY1);
		log.info("Scan  table " + TABLE_NAME + "...");
		System.out.println("Scan  table " + TABLE_NAME + "...");
		System.err.append("Scan  table " + TABLE_NAME + "...");

		// initialize the mapper and reducer
		TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan, StockMapper.class, Text.class, LongWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(TABLE_NAME, StockReducer.class, job);

		// launch the job
		return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new StockDriver(), args);
		System.exit(exitCode);
    }
}
