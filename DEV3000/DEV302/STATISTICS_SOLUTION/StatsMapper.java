package WholeJob;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;


public class StatsMapper  extends Mapper <LongWritable,Text,Text,IntWritable> {
   public static int var1Mean;
   public static int var2Mean;
   public static String var1 = null;
   public static String var2 = null;

   public void setup(Context context) throws IOException, InterruptedException{
      Configuration conf = context.getConfiguration();
      var1 = context.getConfiguration().get("var1");
      var2 = context.getConfiguration().get("var2");
      Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
      String line;
      String[] lineArray;
      BufferedReader cacheReader=null;

      try {
         cacheReader = new BufferedReader(new FileReader(localFiles[0].toString()));
         while((line=cacheReader.readLine()) != null) {
            if (line.contains(var1 + "_mean")) {
               lineArray = line.split("\\t");          
               var1Mean = Integer.parseInt(lineArray[1]);
            }
            else if (line.contains(var2 + "_mean")) {
               lineArray = line.split("\\t");          
               var2Mean = Integer.parseInt(lineArray[1]);
            }
         }
      }
      catch(Exception e) { e.printStackTrace(); }
      finally { cacheReader.close(); }
   }

   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String firstVar = null;
      String secondVar = null;
      int var1Int = 0, var2Int = 0;

      // create iterator over record assuming line-separated fields 
      String valueString = value.toString();
      if(! valueString.contains("%") && valueString.contains(var1) &&  valueString.contains(var2)) {
         StringTokenizer iterator = new StringTokenizer(valueString,"\n");
         String[] valueStringArray;
         String tempString = null;

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

         // pull out value from line
         valueStringArray = tempString.split("\\s+");
         tempString = valueStringArray[3];

         // pull right parens from value
         valueStringArray = tempString.split("\\)"); 
         tempString = valueStringArray[0];
         int firstVarInt = (new Integer(tempString)).intValue();

         // pull out line for var2 
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

         // pull out value from line
         valueStringArray = tempString.split("\\s+");
         tempString = valueStringArray[3];

         // pull right parens from value
         valueStringArray = tempString.split("\\)"); 
         tempString = valueStringArray[0];
         int secondVarInt = (new Integer(tempString)).intValue();

         if (var1 == firstVar) {
            var1Int = firstVarInt;
            var2Int = secondVarInt;
         }
         else {
            var1Int = secondVarInt;
            var2Int = firstVarInt;
         }

         // compute product
         int productInt = (var1Mean - var1Int) * (var2Mean - var2Int);
   
         // emit key-value as  
         context.write(new Text(var1 + "_delta"), new IntWritable(var1Mean - var1Int)); 
         context.write(new Text(var2 + "_delta"), new IntWritable(var2Mean - var2Int)); 
         context.write(new Text("product"), new IntWritable(productInt));
      }
   }
}
