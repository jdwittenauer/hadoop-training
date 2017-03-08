package Voter;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class VoterMapper  extends Mapper <LongWritable,Text,Text,IntWritable> {
   String tempString=null;
   private static Log log = LogFactory.getLog(VoterMapper.class);

   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // create iterator over record assuming space-separated fields
      StringTokenizer iterator = new StringTokenizer(value.toString(), ",");

      // TODO check number of tokens in iterator 
      if (iterator.countTokens() != 6) {
         log.error("incorrect number of tokens:" + value.toString());
         return;
      }

      // TODO eat up the first two tokens 
      iterator.nextToken();
      iterator.nextToken();

      // TODO convert age to an int 
      tempString = iterator.nextToken().toString();
      Integer ageInteger = new Integer(tempString);
      int ageInt = ageInteger.intValue();

      // TODO validate the age is a reasonable age
      if (ageInt < 16 || ageInt > 120) {
         log.error("bad age value:" + value.toString());
         context.getCounter("MYGROUP", "bad_age").increment(1);
         System.err.println("bad age value: " + value.toString());
         return;
      }

      // TODO convert age to an IntWritable
      IntWritable age = new IntWritable(ageInt);

      // TODO pull out party  from record      
      String party = iterator.nextToken().toString();

      // TODO emit key-value as party-age
      context.write(new Text(party), age); 
   }
}
