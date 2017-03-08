package WholeJob;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;


public class UniversityReducer  extends Reducer <Text,IntWritable,Text,IntWritable> {
   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
   int temp = 0;
   int sum = 0;
   int count = 0;
   int min = Integer.MAX_VALUE;
   int max = Integer.MIN_VALUE;

   for (IntWritable value: values) {
      temp = value.get(); 
      
      if (temp < min) {
         min=temp;
      }

      if (temp > max) {
         max = temp;
      }
      sum += temp;
      count++; 
   }

   int mean = sum / count;

   context.write(new Text(key.toString() + "_" + "min"), new IntWritable(min)); 
   context.write(new Text(key.toString() + "_" + "max"), new IntWritable(max)); 
   context.write(new Text(key.toString() + "_" + "mean"), new IntWritable(mean)); 
   }
}
