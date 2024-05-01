import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class TotalTermInDoc {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text docid = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length != 3 || !parts[2].contains(".")) {
                return;
            }
            docid.set(parts[1]);
            DoubleWritable frequency = new DoubleWritable(Double.parseDouble(parts[2]));
            context.write(docid, frequency);
        }
    }

    public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(TotalTermInDoc.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.submit();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
