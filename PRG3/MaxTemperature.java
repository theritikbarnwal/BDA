import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

    public static class TempMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final Text year = new Text();
        private final IntWritable temperature = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            year.set(parts[0]);
            temperature.set(Integer.parseInt(parts[1]));
            context.write(year, temperature);
        }
    }

    public static class TempReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;
            for (IntWritable val : values)
                max = Math.max(max, val.get());

            context.write(key, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Max Temperature");
        job.setJarByClass(MaxTemperature.class);

        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
