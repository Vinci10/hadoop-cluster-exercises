package hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class Exercise2 {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);

        public void map(LongWritable lineNumber,
                        Text lineOfText,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            if (lineNumber.get() == 0L) {
                return;
            }
            String[] splitParts = lineOfText.toString().split(",");
            String stateCode = splitParts[splitParts.length - 1];
            output.collect(new Text(stateCode), one);
        }

    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text stateCode,
                           Iterator<IntWritable> stateOccurrences,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sumOfStates = 0;
            while (stateOccurrences.hasNext()) {
                sumOfStates += stateOccurrences.next().get();
            }

            if (sumOfStates > 5) {
                output.collect(stateCode, new IntWritable(sumOfStates));
            }
        }
    }

    public static class Combine extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text stateCode,
                           Iterator<IntWritable> stateOccurrences,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sumOfStates = 0;
            while (stateOccurrences.hasNext()) {
                sumOfStates += stateOccurrences.next().get();
            }
            output.collect(stateCode, new IntWritable(sumOfStates));
        }

    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Exercise2.class);
        conf.setJobName("hadoop-exercise-2");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combine.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }

}