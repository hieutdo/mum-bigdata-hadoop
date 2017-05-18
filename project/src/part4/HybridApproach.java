package part4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import common.Pair;
import part2.PairsApproach;

import java.io.IOException;

import static common.Utils.formatDouble;

public class HybridApproach {
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
                }
            }
        }
    }

    public static class Reduce extends Reducer<Pair, IntWritable, Text, MapWritable> {
        private MapWritable H;
        private String tPrev;

        private void emit(Context context) throws IOException, InterruptedException {
            double total = 0;

            for (Writable term : H.keySet()) {
                DoubleWritable count = (DoubleWritable) H.get(term);
                total += count.get();
            }

            for (Writable term : H.keySet()) {
                DoubleWritable count = (DoubleWritable) H.get(term);
                count.set(formatDouble(count.get() / total));
            }

            context.write(new Text(tPrev), H);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            H = new MapWritable();
            tPrev = null;
        }

        @Override
        public void reduce(Pair pair, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            String t = pair.getLeft();

            if (!t.equals(tPrev) && tPrev != null) {
                emit(context);
                H.clear();
            }

            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }

            H.put(new Text(pair.getRight()), new DoubleWritable(sum));
            tPrev = t;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            emit(context);
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

        Job job = Job.getInstance(conf, "HybridApproach");

        job.setJarByClass(HybridApproach.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(PairsApproach.Partition.class);

        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }
}
