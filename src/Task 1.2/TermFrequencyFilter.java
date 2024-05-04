import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TermFrequencyFilter {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text termId = new Text();
        private DoubleWritable frequency = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if(key.get() == 0 || key.get() == 1) {
                return;
            }
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 3) {
                termId.set(parts[0]);
                frequency.set(Double.parseDouble(parts[2]));
                context.write(termId, frequency);
            }
        }
    }

    public static class FrequencySumReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            if (sum >= 3.0) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "term frequency filter");
        job.setJarByClass(TermFrequencyFilter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(FrequencySumReducer.class);
        job.setReducerClass(FrequencySumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

