package hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class Exercise3 {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output,
                        Reporter reporter) throws IOException {
            if (key.get() == 0L) {
                return;
            }
            String line = value.toString();
            String[] tokens = line.split(",");
            if (tokens.length != 9) {
                return;
            }
            float finalGrade = Float.parseFloat(tokens[7]);
            String grade = tokens[8];
            output.collect(new Text(grade), new FloatWritable(finalGrade));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key,
                           Iterator<FloatWritable> values,
                           OutputCollector<Text, FloatWritable> output,
                           Reporter reporter) throws IOException {
            float sum = 0;
            int counter = 0;
            while (values.hasNext()) {
                counter++;
                sum += values.next().get();
            }

            output.collect(key, new FloatWritable(sum / counter));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Exercise3.class);
        conf.setJobName("hadoop-exercise-3");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FloatWritable.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }

}