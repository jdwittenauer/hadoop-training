package WholeJob;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;


public class UniversityMapper  extends Mapper <LongWritable,Text,Text,IntWritable> {
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String var1 = context.getConfiguration().get("var1");
      String var2 = context.getConfiguration().get("var2");
      String firstVar = null;
      String secondVar = null;

      // create iterator over record assuming line-separated fields 
      String valueString = value.toString();
      if (!valueString.contains("%") && valueString.contains(var1) &&  valueString.contains(var2)) {
         StringTokenizer iterator = new StringTokenizer(valueString,"\n");

         String[] valueStringArray;
         String tempString = null;
         int tempLength = 0; 

         // pull out line for firstVar 
         while(iterator.hasMoreTokens()) {
            tempString = iterator.nextToken().toString();
            if (tempString.contains(var1)) {
               firstVar = var1; 
               break;
            }
            else if (tempString.contains(var2)) {
               firstVar = var2; 
               break;
            }
         }

         // pull out value for firstVar 
         valueStringArray = tempString.split("\\s+");
         tempString = valueStringArray[3];

         // pull right parens from value
         valueStringArray = tempString.split("\\)"); 
         tempString = valueStringArray[0];
         int firstVarValue = (new Integer(tempString)).intValue();

         // pull out line for secondVar 
         while(iterator.hasMoreTokens()) {
            tempString = iterator.nextToken().toString();
            if (tempString.contains(var1)) {
               secondVar = var1;
               break;
            }
            else if (tempString.contains(var2)) {
               secondVar = var2;
               break;
            }
         }

         // pull out value for secondVar
         valueStringArray = tempString.split("\\s+");
         tempString = valueStringArray[3];

         // pull right parens from value
         valueStringArray = tempString.split("\\)"); 
         tempString = valueStringArray[0];
         int secondVarValue = (new Integer(tempString)).intValue();

         // emit key-value as  
         context.write(new Text(firstVar), new IntWritable(firstVarValue)); 
         context.write(new Text(secondVar), new IntWritable(secondVarValue)); 
      }
   }
}
