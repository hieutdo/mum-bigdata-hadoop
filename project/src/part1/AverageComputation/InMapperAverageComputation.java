package part1.AverageComputation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;

public class InMapperAverageComputation {

    public static class Map extends Mapper<LongWritable, Text, Text, SumCountWritable> {
        private HashMap<String, SumCountWritable> H;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            H = new HashMap<>();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = SimpleAverageComputation.pattern.matcher(line);

            if (matcher.find()) {
                String ip = matcher.group(1);
                int bytes = Integer.parseInt(matcher.group(3), 10);
                SumCountWritable sumCount = H.get(ip);

                if (sumCount == null) {
                    H.put(ip, new SumCountWritable(new LongWritable(bytes), new IntWritable(1)));
                } else {
                    sumCount.getSum().set(sumCount.getSum().get() + bytes);
                    sumCount.getCount().set(sumCount.getCount().get() + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Entry<String, SumCountWritable> entry : H.entrySet()) {
                context.write(new Text(entry.getKey()), entry.getValue());
            }
        }
    }

    public static class Reduce extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<SumCountWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            for (SumCountWritable val : values) {
                sum += val.getSum().get();
                count += val.getCount().get();
            }
            context.write(key, new DoubleWritable(sum / count));
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

        Job job = Job.getInstance(conf, "InMapperAverageComputation");

        job.setJarByClass(InMapperAverageComputation.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCountWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }
}