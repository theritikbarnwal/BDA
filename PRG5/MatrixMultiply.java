import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiply {

    public static class MatrixMapper
            extends Mapper<Object, Text, Text, Text> {

        private int rowsA;
        private int colsB;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            rowsA = conf.getInt("rowsA", 1);
            colsB = conf.getInt("colsB", 1);
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length != 4) return;

            String matrix = parts[0];
            int row = Integer.parseInt(parts[1]);
            int col = Integer.parseInt(parts[2]);
            String val = parts[3];

            if (matrix.equals("A")) {
                for (int j = 0; j < colsB; j++) {
                    context.write(
                        new Text(row + "," + j),
                        new Text("A," + col + "," + val)
                    );
                }
            } else if (matrix.equals("B")) {
                for (int i = 0; i < rowsA; i++) {
                    context.write(
                        new Text(i + "," + col),
                        new Text("B," + row + "," + val)
                    );
                }
            }
        }
    }

    public static class MatrixReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<Integer, Integer> mapA = new HashMap<>();
            Map<Integer, Integer> mapB = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");

                if (parts[0].equals("A")) {
                    mapA.put(
                        Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2])
                    );
                } else {
                    mapB.put(
                        Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2])
                    );
                }
            }

            int result = 0;

            for (Map.Entry<Integer, Integer> entry : mapA.entrySet()) {
                int k = entry.getKey();
                if (mapB.containsKey(k)) {
                    result += entry.getValue() * mapB.get(k);
                }
            }

            context.write(key, new Text(String.valueOf(result)));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: MatrixMultiply <input> <output> <rowsA> <colsB>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.setInt("rowsA", Integer.parseInt(args[2]));
        conf.setInt("colsB", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(MatrixMultiply.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
