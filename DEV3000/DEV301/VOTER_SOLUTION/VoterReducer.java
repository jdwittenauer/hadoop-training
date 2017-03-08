package Voter;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;


public class VoterReducer  extends Reducer <Text,IntWritable,Text,FloatWritable> {
   private static IntWritable tempAge=new IntWritable(0);
   private static Text keyText=new Text();
   private static Text minAge=new Text();
   private static Text maxAge=new Text();
   private static String tempString;
   private static String[] keyString;

   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
     long sum = 0L;
     int count = 0;
     int tempValue = 0;
     int max = Integer.MIN_VALUE;
     int min = Integer.MAX_VALUE;

     for (IntWritable value: values) {
        tempAge = value;
        tempValue = (new Integer(value.toString())).intValue();

        if (tempValue < min) {
           min = tempValue;
        }

        if (tempValue > max) {
           max = tempValue;
        }

        sum += tempValue;
        count++; 
     }

     float mean = sum / count;

     context.write(key, new FloatWritable(mean)); 
   }
}
