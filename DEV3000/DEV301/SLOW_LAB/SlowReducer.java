package Slow;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;


public class SlowReducer extends Reducer <Text,Text,Text,FloatWritable> {
   private static Text tempYear=new Text();
   private static Text keyText=new Text();
   private static Text minYear=new Text();
   private static Text maxYear=new Text();
   private static String tempString;
   private static String[] keyString;

   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
     long sum = 0L;
     int count = 0;
     long tempValue = 0L;
     long max = Long.MIN_VALUE;
     long min = Long.MAX_VALUE;

     for (Text value: values) {
         tempString = value.toString();
         keyString = tempString.split("_"); 
         tempYear = new Text(keyString[0]);
         tempValue = new Long(keyString[1]).longValue(); 

        if(tempValue < min) {
            min = tempValue;
            minYear = tempYear;
         }

        if(tempValue > max) {
            max = tempValue;
            maxYear = tempYear;
        }
        sum += tempValue;
        count++; 
      
     }

     float mean = sum / count;

     keyText.set("min" + "(" + minYear.toString() + "): ");
     context.write(keyText, new FloatWritable(min)); 
     keyText.set("max" + "(" + maxYear.toString() + "): ");
     context.write(keyText, new FloatWritable(max)); 
     keyText.set("mean:");
     context.write(keyText, new FloatWritable(mean)); 
   }
}
