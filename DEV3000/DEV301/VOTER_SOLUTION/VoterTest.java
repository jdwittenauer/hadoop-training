package Voter;

import org.junit.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;


public class VoterTest {
   private static MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

   // TODO declare the reduceDriver object
   private static ReduceDriver<Text, IntWritable, Text, FloatWritable> reduceDriver;

   @Before
   private static void setUp() {
      Voter.VoterMapper mapper = new Voter.VoterMapper();
      mapDriver = MapDriver.newMapDriver(mapper);

      // TODO instantiate a reducer object and reducer driver
      Voter.VoterReducer reducer = new Voter.VoterReducer();
      reduceDriver = ReduceDriver.newReduceDriver(reducer);
   }

   @Test
   private static void testMapper(LongWritable key, Text value, String output) throws IOException {
      String[] outputStringArray = output.split(" ");
      String outputKey = outputStringArray[0];
      String outputValue = outputStringArray[1];
      mapDriver
         .withInput(key, value)
         .withOutput(new Text(outputKey), new IntWritable(Integer.parseInt(outputValue))) 
         .runTest();
   }

   @Test
   private static void testReducer(Text key, List<IntWritable> values, String output) throws IOException {
      // TODO implement the testReducer method
      String[] outputStringArray = output.split("\\s+");
      String outputKey = outputStringArray[0];
      String outputValue = outputStringArray[1];
      reduceDriver
         .withInput(key, values)
         .withOutput(new Text(outputKey), new FloatWritable(Float.parseFloat(outputValue))) 
         .runTest();
   }

   public static void main(String[] args) {
      if (args.length != 2) {
        System.err.printf("usage: %s <map | reduce> <inputfile>\n", "ReceiptsTest"); 
        System.exit(1);
      }
      
      String value=null, output=null;
      BufferedReader reader;
      try {
         reader = new BufferedReader(new FileReader(args[1]));
         value = reader.readLine();
         output = reader.readLine();
      } 
      catch(IOException e) { System.out.println("error reading from input file " + e.toString()); }

      setUp();
      if (args[0].equals("map")) {
         try { testMapper(new LongWritable(0), new Text(value), output); }
         catch (Exception e) { System.err.println("error running test: " + value.toString() + " " + output); }
         finally { System.out.println("success"); } 
      }
     
      else if (args[0].equals("reduce")) {
         // TODO create a list object for reduce input
         List<IntWritable> reduceInput = new ArrayList<IntWritable>();

         // TODO tokenize the first line from the input file 
         StringTokenizer iterator = new StringTokenizer(value, " ");

         // TODO pull out the key from the tokenized line
         Text key = new Text(iterator.nextToken());

         // TODO loop through tokens to add IntWritables to reduce input list
         while(iterator.hasMoreTokens()) 
            reduceInput.add(new IntWritable(Integer.parseInt(iterator.nextToken())));

         try { testReducer(key, reduceInput, output); }
         catch (Exception e) { System.err.println("error running test: " + value.toString() + " " + output); }
         finally { System.out.println("success"); } 
      }

      return;
   }
} 
