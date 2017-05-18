package part1.AverageComputation;

import common.SumCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class InMapper {

    public static class Map extends Mapper<LongWritable, Text, Text, SumCount> {
        private HashMap<String, SumCount> H;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            H = new HashMap<>();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = Simple.pattern.matcher(line);

            if (matcher.find()) {
                String ip = matcher.group(1);
                int bytes = matcher.group(3).equals("-")
                        ? 0
                        : Integer.parseInt(matcher.group(3), 10);
                SumCount sumCount = H.get(ip);

                if (sumCount == null) {
                    H.put(ip, new SumCount(bytes, 1));
                } else {
                    sumCount.setSum(sumCount.getSum() + bytes);
                    sumCount.setCount(sumCount.getCount() + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Entry<String, SumCount> entry : H.entrySet()) {
                context.write(new Text(entry.getKey()), entry.getValue());
            }
        }
    }

    public static class Reduce extends Reducer<Text, SumCount, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            for (SumCount val : values) {
                sum += val.getSum();
                count += val.getCount();
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

        job.setJarByClass(InMapper.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
    }
}