package WholeJob;

import java.util.*;
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


public class UniversityDriver extends Configured implements Tool {
   public int run(String[] args) throws Exception {
      // check the CLI
      if (args.length != 2) {
         System.err.println("usage: java -cp `hbase classpath`:University.jar University.UniversityDriver <inputfile> <outputdir>");
         System.exit(1);
      }

      // setup the Job   
      getConf().set("textinputformat.record.delimiter","))");
      Job job = new Job(getConf(), "university" + System.getProperty("user.name"));
      job.setJarByClass(UniversityDriver.class);
      job.setMapperClass(UniversityMapper.class);
      job.setReducerClass(UniversityReducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      // setup input and output paths
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1])); 
      String var1 = getConf().get("var1");
      String var2 = getConf().get("var2");
      System.out.println("var1 is " + var1);
      System.out.println("var2 is " + var2);

      return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception { 
      Configuration conf = new Configuration();
      System.exit(ToolRunner.run(conf, new UniversityDriver(), args));
   } 
}
