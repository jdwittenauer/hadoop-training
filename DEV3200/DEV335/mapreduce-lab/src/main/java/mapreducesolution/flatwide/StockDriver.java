package mapreducesolution.flatwide;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StockDriver extends Configured implements Tool {
    public static final String userdirectory = ".";
    public static final String TABLE_NAME = userdirectory + "/trades_flat";
    protected final static String PRICE = "price";
    protected final static byte[] COLUMN_FAMILY_PRICE = Bytes.toBytes(PRICE);
    protected final static String MEAN = "mean";
    protected final static String MAX = "max";
    protected final static String MIN = "min";
    protected final static String STATS = "stats";
    protected final static byte[] COLUMN_FAMILY_STATS = Bytes.toBytes(STATS);
    protected final static byte[] COL_QUALIFIER_MEAN = Bytes.toBytes(MEAN);
    protected final static byte[] COL_QUALIFIER_MIN = Bytes.toBytes(MIN);
    protected final static byte[] COL_QUALIFIER_MAX = Bytes.toBytes(MAX);

    public int run(String[] args) throws Exception {
        // setup the Job
        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        // instantiate and initialize the scan object
        Scan scan = new Scan();
        scan.setMaxVersions();
        scan.addFamily(COLUMN_FAMILY_PRICE);

        // initialize the mapper and reducer
        TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan, StockMapper.class, Text.class, LongWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, StockReducer.class, job);

        // launch the job and block waiting
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StockDriver(), args);
        System.exit(exitCode);
    }
}
