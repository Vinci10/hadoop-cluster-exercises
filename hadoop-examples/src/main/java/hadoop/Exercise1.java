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
import java.util.StringTokenizer;

public class Exercise1 {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key,
                        Text text,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            String lineOfText = text.toString();
            StringTokenizer tokenizer = new StringTokenizer(lineOfText, ",");
            IntWritable one = new IntWritable(1);
            while (tokenizer.hasMoreTokens()) {
                String stateCode = tokenizer.nextToken().trim();
                if (stateCode.length() == 2
                        && stateCode.charAt(0) >= 'A'
                        && stateCode.charAt(0) <= 'Z'
                        && stateCode.charAt(1) >= 'A'
                        && stateCode.charAt(1) <= 'Z') {
                    output.collect(new Text(stateCode), one);
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key,
                           Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sumOfCitiesPerState = 0;
            while (values.hasNext()) {
                sumOfCitiesPerState += values.next().get();
            }

            output.collect(key, new IntWritable(sumOfCitiesPerState));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Exercise1.class);
        conf.setJobName("hadoop-exercise-1");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }

}