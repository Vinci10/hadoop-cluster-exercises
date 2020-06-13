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

import java.io.IOException;
import java.util.Iterator;

public class Exercise3 {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Average> {

        public void map(LongWritable lineNumber,
                        Text lineOfText,
                        OutputCollector<Text, Average> output,
                        Reporter reporter) throws IOException {
            if (lineNumber.get() == 0L) {
                return;
            }
            String[] splitParts = lineOfText.toString().split(",");
            String grade = splitParts[splitParts.length - 1];
            String finalGrade = splitParts[splitParts.length - 2].trim();
            float finalGradeNumber = Float.parseFloat(finalGrade);
            output.collect(new Text(grade), new Average(finalGradeNumber, 1));
        }

    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Average, Text, Average> {

        public void reduce(Text key,
                           Iterator<Average> values,
                           OutputCollector<Text, Average> output,
                           Reporter reporter) throws IOException {
            float sum = 0;
            int count = 0;

            while (values.hasNext()) {
                Average record = values.next();
                sum += record.getNumber() * record.getCount();
                count += record.getCount();
            }

            if (count != 0) {
                float avg = sum / count;
                output.collect(key, new Average(avg, count));
            }
        }

    }

    public static class Combine extends MapReduceBase implements Reducer<Text, Average, Text, Average> {

        public void reduce(Text key,
                           Iterator<Average> values,
                           OutputCollector<Text, Average> output,
                           Reporter reporter) throws IOException {
            float sum = 0;
            int count = 0;

            while (values.hasNext()) {
                Average record = values.next();
                sum += record.getNumber();
                count += record.getCount();
            }

            if (count != 0) {
                float avg = sum / count;
                output.collect(key, new Average(avg, count));
            }

        }

    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Exercise3.class);
        conf.setJobName("hadoop-exercise-3");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Average.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setCombinerClass(Combine.class);
        conf.setInputFormat(TextInputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }

}