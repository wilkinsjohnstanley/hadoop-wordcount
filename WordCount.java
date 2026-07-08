import java.io.IOException;
import java.util.StringTokenizer;
// Standard I/O exceptions and a class to split text into words.
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//Hadoop's MadReduce API. Text / IntWritable are Hadoop's serializable wrappers for String/int
//Needed because data gets shipped across the network between notes.
public class WordCount {
//Everything lives in one class. Mapper, Reducer, and the driver (main) as nested classes.
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
//Generic types = <input key, input value, output key, output value>. Input key is the byte offset in the file (unused, hence Object); input value is a line of text. Output is (word, 1).
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
//Reused objects (avoids allocating a new one per word (a Hadoop performance vention))
    public void map(Object key, Text value, Context context
                     ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
//For each line we split on whitespace as the delimiter, and then for every token, we emit something like (word, 1) via the context.write
  //This is the reducer:
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
// This takes all the values Hadoop grouped under the same key (word) and sums them. 
    public void reduce(Text key, Iterable<IntWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
//This is the DRIVER.
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    //We load Hadoop's config (the XML files we set up), and it creates a job named "word count"
    job.setJarByClass(WordCount.class);
    //This tells Hadoop which JAR to ship to the cluster (found via this class)
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    /*This wires up the classes. The combiner is an optimization.
    It runs a local mini-reduce on each mapper's output before shuffling data across the network, cutting traffic.
    We reuse the reducer as the combiner since summing is associative.*/
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //This declares the final output types (word, count).
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //Input/output HDFS paths. These are the two args we passed: /user/hadoop/input and /user/hadoop/output
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    //Runs the job, blocks until done, exists 0 on success or 1 on failure. This is the standard convention for shell/scripting integration. 
  }
}
