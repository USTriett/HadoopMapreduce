import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopFrequency {

    public static class MatrixTopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Double, Text> topTenMap = new TreeMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");

            // Skip the header line
            if (tokens[0].startsWith("%%"))
                return;

            if (tokens.length != 3)
                return; // Malformed line, skip

            int row = Integer.parseInt(tokens[0]);
            int col = Integer.parseInt(tokens[1]);
            double val = Double.parseDouble(tokens[2]);

            // Formulate the output value as "row col val"
            Text outputValue = new Text(row + " " + col + " " + val);

            // Store the value in the TreeMap with val as key
            topTenMap.put(val, outputValue);

            // Keep only top 10 values
            if (topTenMap.size() > 10) {
                topTenMap.remove(topTenMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit the top 10 values
            for (Text value : topTenMap.values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static class MatrixTopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private TreeMap<Double, Text> topTenMap = new TreeMap<>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                // Split the value into its components
                String[] components = value.toString().split("\\s+");
                if (components.length != 3)
                    continue; // Malformed line, skip

                double val = Double.parseDouble(components[2]);

                // Formulate the output value as "row col val"
                Text outputValue = new Text(components[0] + " " + components[1] + " " + val);

                // Store the value in the TreeMap with val as key
                topTenMap.put(val, outputValue);

                // Keep only top 10 values
                if (topTenMap.size() > 10) {
                    topTenMap.remove(topTenMap.firstKey());
                }
            }

            // Emit the top 10 values
            for (Text value : topTenMap.descendingMap().values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Ten Frequencies");
        job.setJarByClass(TopFrequency.class);
        job.setMapperClass(MatrixTopTenMapper.class);
        job.setReducerClass(MatrixTopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
