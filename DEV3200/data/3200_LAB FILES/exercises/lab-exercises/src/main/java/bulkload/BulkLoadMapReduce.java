package bulkload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Persist data into HBase with MapReduce. Depending on command line parameters
 * output data to table using TableOutputFormat or save to HFile. To save the
 * data to an HTable invoke this job with the following arguments (replace
 * user01 with your user name): /user/user19/hly_temp /user/user19/input
 *
 */
public class BulkLoadMapReduce {

	// TODO create table in HBase shell (replace user01 with your user name): 
    // 		create '/user/user19/hly_temp', 'readings'
    //
    // Upload the data file hly-temp-10pctl.txt to the cluster via scp 
    // to your user directory e.g. /user/user01
    //
    // Create an input directory
    // 		mkdir /user/user19/input
    //
    // Copy the data file (hly_temp) into the newly created directory
    //
    static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        // time stamp for all inserted rows
        private long ts;

        // column family name
        static byte[] family = Bytes.toBytes("readings");

        @Override
        protected void setup(Context context) {
            ts = System.currentTimeMillis();
        }

        @Override
        public void map(LongWritable offset, Text value, Context context)
                throws IOException {
            try {

                String line = value.toString();
                // TODO :  Create the following using the instructions in the PDF.
                String stationID = line.substring(0, 11);
                String month = "TODO";
                String day = "TODO";
                String rowkey = "TODO";
                byte[] bRowKey = Bytes.toBytes(rowkey);
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);

                // TODO Create Put instance
                Put p = null;

                for (int i = 1; i < 25; i++) {
                    // column name is value00x where x = i
                    String columnName = "v" + leftPad(String.valueOf(i), 2, '0');
                    int beginIndex = i * 7 + 11;
                    // grab each subsequent value there are up to 24 values per line
                    String temperature = line.substring(beginIndex, beginIndex + 6).trim();

					//TODO Add the data to the put instance p
                    //cf, col, timestamp-ts, temperature
                    //p.add ...
                }
                context.write(rowKey, p);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static String leftPad(String str, int length, char pad) {
        return String.format("%1$" + length + "s", str).replace(' ', pad);
    }

    /**
     * Sets up the actual job.
     *
     * @param conf The current configuration.
     * @param args The command line parameters.
     * @return The newly created job.
     * @throws IOException When setting up the job fails.
     */
    public static Job createSubmittableJob(Configuration conf, String[] args)
            throws IOException {

        if (args.length == 0) {
            System.out.println("Usage for setup: java -cp `hbase classpath`:./lab-exercises-1.1.jar bulkload.BulkloadMapReduce <TABLE> <DIR PATH to the input file>   ");
            System.out.println(" java -cp `hbase classpath`:./lab-exercises-1.1.jar  bulkload.BulkLoadMapReduce /user/user19/hly_temp /user/user19/input/");
        }

        // TODO   set the table name and inputDir passed in as arguments
        String tableName = "TODO";
        Path inputDir = new Path("TODO");
        Job job = new Job(conf, "bulk_load_mapreduce");
        job.setJarByClass(ImportMapper.class);
        FileInputFormat.setInputPaths(job, inputDir);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ImportMapper.class);

        if (args.length < 3) {
            // Insert into table directly using TableOutputFormat 
            TableMapReduceUtil.initTableReducerJob(tableName, null, job);
            job.setNumReduceTasks(0);
        } else {
            // Otherwise generate HFile 
            HTable table = new HTable(conf, tableName);
            job.setReducerClass(PutSortReducer.class);
            Path outputDir = new Path(args[2]);
            FileOutputFormat.setOutputPath(job, outputDir);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            HFileOutputFormat.configureIncrementalLoad(job, table);
        }

        TableMapReduceUtil.addDependencyJars(job);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = createSubmittableJob(conf, args);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
