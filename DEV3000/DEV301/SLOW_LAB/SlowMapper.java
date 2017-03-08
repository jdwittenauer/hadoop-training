package Slow;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class SlowMapper extends Mapper <LongWritable,Text,Text,Text> {
   private final Text tempText = new Text();

   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // create iterator over record assuming space-separated fields
      StringTokenizer iterator = new StringTokenizer(value.toString(), " ");
      
      // pull out year from record
      String year = new String(iterator.nextToken()).toString();

      // pull out sleep from conf
      int sleepTime = Integer.parseInt(context.getConfiguration().get("my.map.sleep"));
      Thread.sleep(sleepTime);

      // pull out 3rd field from record      
      long surplus_or_deficit = 0L;
      iterator.nextToken();
      iterator.nextToken();
      tempText.set(iterator.nextToken());  
      context.write(new Text("summary"), new Text(year + "_" + tempText.toString()));
   }
}
