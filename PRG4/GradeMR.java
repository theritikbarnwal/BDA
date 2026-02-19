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

        private final Text name = new Text();
        private final Text grade = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length != 2) return; // avoid crash

            int marks;
            try {
                marks = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                return; // skip bad record
            }

            name.set(parts[0].trim());

            if (marks >= 80) grade.set("A");
            else if (marks >= 60) grade.set("B");
            else if (marks >= 50) grade.set("C");
            else grade.set("D");

            context.write(name, grade);
        }
    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Student Grade");
        job.setJarByClass(GradeMR.class);

        job.setMapperClass(GradeMapper.class);

        job.setNumReduceTasks(0); // IMPORTANT

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
