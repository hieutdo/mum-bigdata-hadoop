package part3;

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

import java.io.IOException;

public class SimpleStripesApproach {
    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] terms = value.toString().split("\\s+");

            for (int i = 0; i < terms.length - 1; i++) {
                MapWritable H = new MapWritable();

                for (int j = i + 1; j < terms.length; j++) {
                    if (terms[j].equals(terms[i])) {
                        break;
                    }

                    Text neighbor = new Text(terms[j]);

                    if (H.containsKey(neighbor)) {
                        IntWritable count = (IntWritable) H.get(neighbor);
                        count.set(count.get() + 1);
                    } else {
                        H.put(neighbor, new IntWritable(1));
                    }
                }

                if (!H.isEmpty()) {
                    context.write(new Text(terms[i]), H);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable Hf = new MapWritable();

            for (MapWritable H : values) {
                for (Writable neighbor : H.keySet()) {
                    if (Hf.containsKey(neighbor)) {
                        IntWritable totalCount = (IntWritable) Hf.get(neighbor);
                        IntWritable neighborCount = (IntWritable) H.get(neighbor);
                        totalCount.set(totalCount.get() + neighborCount.get());
                    } else {
                        Hf.put(neighbor, H.get(neighbor));
                    }
                }
            }

            context.write(key, Hf);
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

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }
}
