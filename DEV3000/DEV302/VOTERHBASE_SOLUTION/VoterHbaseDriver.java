package VoterHbase; 

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class VoterHbaseDriver extends Configured implements Tool {
   protected static byte[] INPUT_TABLE_NAME;

   public int run(String[] args) throws Exception {
      // check the CLI
      if (args.length != 2) {
         System.err.println("usage: java -cp `hbase classpath`:VoterHbase.jar VoterHbase.VoterHbaseDriver <input-table> <outputdir>");
         System.exit(1);
      }

      INPUT_TABLE_NAME = Bytes.toBytes(args[0]);

      // setup the job   
      Job job = Job.getInstance(getConf(), getClass().getSimpleName());
      job.setJarByClass(getClass());
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      // instantiate and initialize the scan object
      Scan scan = new Scan();
      scan.setMaxVersions();
      scan.addFamily(Bytes.toBytes("cf1"));
      scan.addFamily(Bytes.toBytes("cf2"));

      // initialize the mapper and reducer
      TableMapReduceUtil.initTableMapperJob(INPUT_TABLE_NAME, scan, VoterHbaseMapper.class, Text.class, IntWritable.class, job);
      job.setReducerClass(VoterHbaseReducer.class);

      // launch the job
      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception { 
      Configuration conf = new Configuration();
      System.exit(ToolRunner.run(conf, new VoterHbaseDriver(), args));
   } 
}
