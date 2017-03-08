package Stats; 

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class StatsDriver extends Configured implements Tool {
   public int run(String[] args) throws Exception {
      // check the CLI
      if (args.length != 2) {
         System.err.println("usage: java -cp `hbase classpath`:Stats.jar Stats.StatsDriver <inputfile> <outputdir>");
         System.exit(1);
      }

      // setup the job   
      getConf().set("textinputformat.record.delimiter","))");
      Job job = new Job(getConf(), "stats" + System.getProperty("user.name"));
      job.setJarByClass(StatsDriver.class);
      job.setMapperClass(StatsMapper.class);
      job.setReducerClass(StatsReducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      // setup input and output paths
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1])); 

      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception { 
      Configuration conf = new Configuration();
      System.exit(ToolRunner.run(conf, new StatsDriver(), args));
   } 
}
