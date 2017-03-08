package University;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;


public class UniversityMapper extends Mapper <LongWritable,Text,Text,IntWritable> {
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // create iterator over record assuming line-separated fields 
      String valueString = value.toString();
      if(valueString.contains("sat verbal") && valueString.contains("sat math")) {
      StringTokenizer iterator = new StringTokenizer(valueString, "\n");
      String[] valueStringArray;
      String tempString=null;

      // pull out line for percent-financial-aid from record
      while(iterator.hasMoreTokens()) {
         tempString = iterator.nextToken().toString();
         if (tempString.contains("verbal"))
            break;
      }

      // pull out value from line
      valueStringArray = tempString.split("\\s+");
      tempString = valueStringArray[3];

      // pull right parens from value
      valueStringArray = tempString.split("\\)"); 
      tempString = valueStringArray[0];

      int satVerbal = (new Integer(tempString)).intValue();

      // pull out line for quality-of-life from record      
      while(iterator.hasMoreTokens()) {
         tempString = iterator.nextToken().toString();
         if (tempString.contains("math"))
            break;
      }

      // pull out value from line
      valueStringArray = tempString.split("\\s+");
      tempString = valueStringArray[3];

      // pull right parens from value
      valueStringArray = tempString.split("\\)"); 
      tempString = valueStringArray[0];

      int satMath = (new Integer(tempString)).intValue();

      // emit key-value as  
      context.write(new Text("satv"), new IntWritable(satVerbal)); 
      context.write(new Text("satm"), new IntWritable(satMath)); 
      }
   }
}
