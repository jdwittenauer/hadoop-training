package WholeJob; 

import java.io.BufferedReader;
import java.io.FileReader;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;


public class WholeJobDriver extends Configured implements Tool {
   public int run(String[] args) throws Exception {
      // declare globals
      Job job;

      // run first job on university data
      getConf().set("textinputformat.record.delimiter","))");
      job = new Job(getConf(), "university" + System.getProperty("user.name"));
      job.setJarByClass(WholeJobDriver.class);
      job.setMapperClass(UniversityMapper.class);
      job.setReducerClass(UniversityReducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      // setup input and output paths
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      boolean returnCode = job.waitForCompletion(true);

      // put results from first job in distributed cache
      FileSystem fs = FileSystem.get(getConf());
      Path resultsFile = new Path(args[1] + "/part-r-00000");
      fs.copyFromLocalFile(resultsFile, new Path("/user/user01/8/DISTRIBUTED_CACHE/stats.txt"));
      DistributedCache.addCacheFile(resultsFile.toUri(), getConf());

      // setup the second job 
      getConf().set("textinputformat.record.delimiter","))");
      job = new Job(getConf(), "stats" + System.getProperty("user.name"));
      job.setJarByClass(WholeJobDriver.class);
      job.setMapperClass(StatsMapper.class);
      job.setReducerClass(StatsReducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      // setup input and output paths
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[2])); 

      boolean returnCode2=job.waitForCompletion(true);
      String var1 = getConf().get("var1");
      String var2 = getConf().get("var2");
      String line;
      String[] lineArray;
      double var1_sumofsquares = 0;
      double var2_sumofsquares = 0;
      double product_sumofsquares = 0;

      try {
         BufferedReader cacheReader = new BufferedReader(new FileReader(args[2] + "/part-r-00000"));

         while((line = cacheReader.readLine()) != null) {
            if (line.contains(var1)) {
               lineArray = line.split("\\s+");
               var1_sumofsquares = Double.parseDouble(lineArray[1]);
            }
            else if (line.contains(var2)) {
               lineArray = line.split("\\s+");
               var2_sumofsquares = Double.parseDouble(lineArray[1]);
            }
            else if (line.contains("product")) {
               lineArray = line.split("\\s+");
               product_sumofsquares = Double.parseDouble(lineArray[1]);
            }
         }
      }
      catch(Exception e) { e.printStackTrace(); }

      double spearmanCoefficient = product_sumofsquares / Math.sqrt(var1_sumofsquares * var2_sumofsquares);
      System.out.println("product_sumofsquares is " + product_sumofsquares);
      System.out.println("var1_sumofsquares is " + var1_sumofsquares);
      System.out.println("var2_sumofsquares is " + var2_sumofsquares);
      System.out.println("spearman's coefficient is " + spearmanCoefficient);      

      return returnCode && returnCode2 ? 0 : 1;
   }

   public static void main(String[] args) throws Exception { 
      Configuration conf = new Configuration();
      System.exit(ToolRunner.run(conf, new WholeJobDriver(), args));
   } 
}
