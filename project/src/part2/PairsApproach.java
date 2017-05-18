package part2;

import common.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import static common.Utils.formatDouble;

public class PairsApproach {
    private static final String STAR = "*";
    private static final IntWritable one = new IntWritable(1);

    public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] terms = value.toString().split("\\s+");

            for (int i = 0; i < terms.length - 1; i++) {
                String term = terms[i];

                for (int j = i + 1; j < terms.length; j++) {
                    String neighbor = terms[j];

                    if (neighbor.equals(term)) {
                        break;
                    }

                    context.write(new Pair(term, neighbor), one);
                    context.write(new Pair(term, STAR), one);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
        private int totalSum = 0;

        @Override
        public void reduce(Pair pair, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable count : counts) {
                sum += count.get();
            }

            if (STAR.equals(pair.getRight())) {
                totalSum = sum;
            } else {
                double average = formatDouble((double) sum / totalSum);
                context.write(pair, new DoubleWritable(average));
            }
        }
    }

    public static class Partition extends Partitioner<Pair, IntWritable> {

        @Override
        public int getPartition(Pair pair, IntWritable count, int numReduceTasks) {
            char firstChar = pair.getLeft().toUpperCase().charAt(0);
            return (firstChar < 'C') ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        Path inputDir = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // delete output dir if exists
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }

        Job job = Job.getInstance(conf, "PairsApproach");

        job.setJarByClass(PairsApproach.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }
}
