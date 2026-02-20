import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GradeMR {

    public static class GradeMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text name = new Text();
        private Text grade = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length != 2) return;

            String student = parts[0].trim();
            int marks = Integer.parseInt(parts[1].trim());

            String result;

            if (marks >= 80) result = "A";
            else if (marks >= 60) result = "B";
            else result = "C";

            name.set(student);
            grade.set(result);

            context.write(name, grade);
        }
    }

    public static class GradeReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Student Grade");

        job.setJarByClass(GradeMR.class);

        job.setMapperClass(GradeMapper.class);
        job.setReducerClass(GradeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);   // IMPORTANT

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}