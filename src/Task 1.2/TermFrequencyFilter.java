import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.*;

public class TermFrequencyFilter {
    public static class PairWritable implements Writable {

        private Text docId;
        private DoubleWritable frequency;

        public PairWritable() {
            this.docId = new Text();
            this.frequency = new DoubleWritable();
        }
        public PairWritable(String docId, double frequency) {
            this.docId = new Text(docId);
            this.frequency = new DoubleWritable(frequency);
        }

        public void set(String docId, double frequency) {
            this.docId = new Text(docId);
            this.frequency = new DoubleWritable(frequency);
        }

        public Text getDocId() {
            return docId;
        }

        public DoubleWritable getFrequency() {
            return frequency;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            docId.write(out);
            frequency.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            docId.readFields(in);
            frequency.readFields(in);
        }

        @Override
        public String toString() {
            return docId.toString() + " " + frequency.toString();
        }
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, PairWritable> {

        private Text termId = new Text();
        private PairWritable pair = new PairWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 3) {
                termId.set(parts[0]);
                pair.set(parts[1],Double.parseDouble(parts[2]));
                context.write(termId, pair);
            }
        }
    }

    public static class FrequencySumReducer
            extends Reducer<Text, PairWritable, Text, PairWritable> {

        public void reduce(Text key, Iterable<PairWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0.0;
            List<PairWritable> cache = new ArrayList<>();

            for (PairWritable val : values) {
                sum += val.getFrequency().get();
                cache.add(new PairWritable(val.getDocId().toString(), val.getFrequency().get()));
            }
            if (sum >= 3.0) {
                for (int i = 0; i < cache.size(); ++i) {
                    context.write(key, cache.get(i));
                }
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
        job.setOutputValueClass(PairWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

