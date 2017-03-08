package VoterHbase;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class VoterHbaseMapper  extends TableMapper <Text,IntWritable> {
   private final Text reusableText = new Text();

   @Override
   public void map(ImmutableBytesWritable key, Result row, Context context) throws IOException, InterruptedException {
      // pull out age from record
      byte[] ageBytes= row.getColumnLatest(Bytes.toBytes("cf2"), Bytes.toBytes("age")).getValue();
      Integer ageInteger = new Integer(new String(ageBytes));
      int ageInt = ageInteger.intValue();
      IntWritable age = new IntWritable(ageInt);

      // pull out party from record      
      byte[] partyBytes = row.getColumnLatest(Bytes.toBytes("cf2"), Bytes.toBytes("party")).getValue();

      // emit key-value as party-age
      context.write(new Text(partyBytes), age); 
   }
}
