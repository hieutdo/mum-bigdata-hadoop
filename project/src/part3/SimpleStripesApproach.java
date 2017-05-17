package part3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class SimpleStripesApproach {
    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] terms = value.toString().split("\\s+");

            for (int i = 0; i < terms.length - 1; i++) {
                String term = terms[i];
                MapWritable H = new MapWritable();

                for (int j = i + 1; j < terms.length; j++) {
                    String neighbor = terms[j];

                    if (neighbor.equals(term)) {
                        break;
                    }

                    Text neighborKey = new Text(neighbor);

                    if (H.containsKey(neighborKey)) {
                        IntWritable count = (IntWritable) H.get(neighborKey);
                        count.set(count.get() + 1);
                    } else {
                        H.put(neighborKey, new IntWritable(1));
                    }
                }

                if (!H.isEmpty()) {
                    context.write(new Text(term), H);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {
        private DecimalFormat decimalFormat = new DecimalFormat("##.###");

        private double formatDouble(double num) {
            return Double.parseDouble(decimalFormat.format(num));
        }

        @Override
        public void reduce(Text term, Iterable<MapWritable> stripes, Context context) throws IOException, InterruptedException {
            MapWritable Hf = new MapWritable();
            int marginal = 0;

            for (MapWritable stripe : stripes) {
                for (Writable neighbor : stripe.keySet()) {
                    IntWritable neighborCount = (IntWritable) stripe.get(neighbor);
                    DoubleWritable totalCount = (DoubleWritable) Hf.get(neighbor);

                    if (totalCount == null) {
                        totalCount = new DoubleWritable(neighborCount.get());
                    } else {
                        totalCount.set(totalCount.get() + neighborCount.get());
                    }

                    marginal += neighborCount.get();

                    Hf.put(neighbor, totalCount);
                }
            }

            for (Writable neighbor : Hf.keySet()) {
                DoubleWritable count = (DoubleWritable) Hf.get(neighbor);
                count.set(formatDouble(count.get() / marginal));
            }

            context.write(term, Hf);
        }
    }

    public static class Partition extends Partitioner<Text, MapWritable> {

        @Override
        public int getPartition(Text term, MapWritable map, int numReduceTasks) {
            char firstChar = term.toString().toUpperCase().charAt(0);
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

        Job job = Job.getInstance(conf, "SimpleStripesApproach");

        job.setJarByClass(SimpleStripesApproach.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

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
