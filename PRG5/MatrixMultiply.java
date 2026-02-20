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

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            rowsA = conf.getInt("rowsA", -1);
            colsB = conf.getInt("colsB", -1);
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length != 4) return;

            String matrix = parts[0].trim();

            int row, col, val;
            try {
                row = Integer.parseInt(parts[1].trim());
                col = Integer.parseInt(parts[2].trim());
                val = Integer.parseInt(parts[3].trim());
            } catch (NumberFormatException e) {
                return; // skip invalid records
            }

            if (matrix.equals("A")) {

                for (int j = 0; j < colsB; j++) {
                    outputKey.set(row + "," + j);
                    outputValue.set("A," + col + "," + val);
                    context.write(outputKey, outputValue);
                }

            } else if (matrix.equals("B")) {

                for (int i = 0; i < rowsA; i++) {
                    outputKey.set(i + "," + col);
                    outputValue.set("B," + row + "," + val);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class MatrixReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<Integer, Integer> mapA = new HashMap<>();
            Map<Integer, Integer> mapB = new HashMap<>();

            for (Text val : values) {

                String[] parts = val.toString().split(",");
                if (parts.length != 3) continue;

                String matrix = parts[0];

                try {
                    int index = Integer.parseInt(parts[1]);
                    int number = Integer.parseInt(parts[2]);

                    if (matrix.equals("A")) {
                        mapA.put(index, number);
                    } else if (matrix.equals("B")) {
                        mapB.put(index, number);
                    }

                } catch (NumberFormatException e) {
                    // skip bad data
                }
            }

            int sum = 0;

            for (Map.Entry<Integer, Integer> entry : mapA.entrySet()) {
                int k = entry.getKey();
                if (mapB.containsKey(k)) {
                    sum += entry.getValue() * mapB.get(k);
                }
            }

            result.set(String.valueOf(sum));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: MatrixMultiply <input> <output> <rowsA> <colsB>");
            System.exit(-1);
        }

        int rowsA, colsB;

        try {
            rowsA = Integer.parseInt(args[2]);
            colsB = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.err.println("rowsA and colsB must be integers.");
            System.exit(-1);
            return;
        }

        Configuration conf = new Configuration();
        conf.setInt("rowsA", rowsA);
        conf.setInt("colsB", colsB);

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