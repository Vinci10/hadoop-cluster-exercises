package hadoop;

import org.apache.hadoop.fs.Path;
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
import java.util.StringTokenizer;

public class Exercise4 {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output,
                        Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            long index = key.get();
            while (tokenizer.hasMoreTokens()) {
                Text val = new Text(tokenizer.nextToken());
                output.collect(new LongWritable(index), val);
                index += val.getLength();
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

        private final Text word = new Text();

        public void reduce(LongWritable key,
                           Iterator<Text> values,
                           OutputCollector<LongWritable, Text> output,
                           Reporter reporter) throws IOException {
            StringBuilder translations = new StringBuilder();

            while (values.hasNext()) {
                translations.insert(0, values.next().toString() + " ");
            }

            word.set(translations.toString());
            output.collect(new LongWritable(0), word);
        }
    }

    public static class Combine extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

        private final Text word = new Text();

        public void reduce(LongWritable key,
                           Iterator<Text> values,
                           OutputCollector<LongWritable, Text> output,
                           Reporter reporter) throws IOException {
            StringBuilder translations = new StringBuilder();

            while (values.hasNext()) {
                translations.insert(0, values.next().toString());
            }

            word.set(translations.toString());
            output.collect(new LongWritable(0), word);
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Exercise4.class);
        conf.setJobName("hadoop-exercise-4");
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);
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