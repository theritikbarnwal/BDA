import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GradeMR {

    public static class GradeMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text name = new Text();
        private Text grade = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length != 2) return;

            int marks;
            try {
                marks = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                return;
            }

            if (marks < 0 || marks > 100) return;

            name.set(parts[0].trim());

            if (marks >= 80) grade.set("A");
            else if (marks >= 60) grade.set("B");
            else if (marks >= 50) grade.set("C");
            else grade.set("D");

            context.write(name, grade);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: GradeMR <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Student Grade");

        job.setJarByClass(GradeMR.class);
        job.setMapperClass(GradeMapper.class);

        job.setNumReduceTasks(0);  // Map-only job

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}