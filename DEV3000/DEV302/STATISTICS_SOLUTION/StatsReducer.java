package WholeJob;

import java.io.IOException;
import java.lang.Math;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;


public class StatsReducer  extends Reducer <Text,IntWritable,Text,LongWritable> {
   public static String var1 = null;
   public static String var2 = null;

   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      long var1_sumofsquares = 0L;
      long var2_sumofsquares = 0L;
      long sumofproducts = 0L;
      int square = 0;
      int temp = 0;

      var1 = context.getConfiguration().get("var1");
      var2 = context.getConfiguration().get("var2");

      if (key.toString().contains(var1 + "_delta")) {
         for (IntWritable value: values) {
            temp = value.get(); 
            var1_sumofsquares += temp * temp;
         }

         context.write(new Text(key.toString() + "_sumofsquares"), new LongWritable(var1_sumofsquares));
      }
      else if (key.toString().contains(var2 + "_delta")) { 
         for (IntWritable value: values) {
            temp = value.get(); 
            var2_sumofsquares += temp * temp;
         }

         context.write(new Text(key.toString() + "_sumofsquares"), new LongWritable(var2_sumofsquares));
      }
      else if (key.toString().contains("product")) {
         for (IntWritable value: values) {
            temp = value.get(); 
            sumofproducts += temp;
         }

         context.write(new Text(key.toString() + "_sumofproducts"), new LongWritable(sumofproducts)); 
      }
   }
}
