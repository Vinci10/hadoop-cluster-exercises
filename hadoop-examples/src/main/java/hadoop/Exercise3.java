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

        public void map(LongWritable lineNumber,
                        Text lineOfText,
                        OutputCollector<Text, FloatWritable> output,
                        Reporter reporter) throws IOException {
            if (lineNumber.get() == 0L) {
                return;
            }
            String[] splitParts = lineOfText.toString().split(",");
            String grade = splitParts[splitParts.length - 1];
            String finalGrade = splitParts[splitParts.length - 1];
            float finalGradeNumber = Float.parseFloat(finalGrade);
            output.collect(new Text(grade), new FloatWritable(finalGradeNumber));
        }

    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key,
                           Iterator<FloatWritable> values,
                           OutputCollector<Text, FloatWritable> output,
                           Reporter reporter) throws IOException {
            float sumOfFinalGradesScore = 0;
            int numberOfGrades = 0;
            while (values.hasNext()) {
                numberOfGrades++;
                sumOfFinalGradesScore += values.next().get();
            }
            if (numberOfGrades != 0) {
                output.collect(key, new FloatWritable(sumOfFinalGradesScore / numberOfGrades));
            }
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