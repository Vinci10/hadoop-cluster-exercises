package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Exercise4v2 {
    private static final String DELIMITER = "/";

    public static class DescendingComparator extends WritableComparator {
        public DescendingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String[] keyA = a.toString().split(DELIMITER);
            String[] keyB = b.toString().split(DELIMITER);
            String keyAFileName = keyA[0];
            String keyBFileName = keyB[0];
            if (keyAFileName.equals(keyBFileName)) {
                long keyAOffset = Long.parseLong(keyA[1]);
                long keyBOffset = Long.parseLong(keyB[1]);
                return -1 * Long.compare(keyAOffset, keyBOffset);
            }
            return super.compare(a, b);
        }
    }

    public static class FileNamePartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String fileName = key.toString().split(DELIMITER)[0];
            return (fileName.toUpperCase().charAt(0) <= 'M' ? 0 : 1) % numReduceTasks;
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            List<String> words = Arrays.asList(line.split("\\s+"));
            Collections.reverse(words);
            String reversedLine = String.join(" ", words) + "\n";
            context.write(new Text(fileName + DELIMITER + key.get()), new Text(reversedLine));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "hadoop-exercise-4");
        job.setPartitionerClass(FileNamePartitioner.class);
        job.setNumReduceTasks(1);
        job.setJarByClass(Exercise4v2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setSortComparatorClass(DescendingComparator.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
