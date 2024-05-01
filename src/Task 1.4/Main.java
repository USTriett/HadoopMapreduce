import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class Main {

    public static class Utils {
        private static HashMap<String, Integer> countDocOfTermMap = new HashMap<>();
        private static HashMap<String, Double> totalTermInDocMap = new HashMap<>();
        private static int totalDocs;

        public static int getTotalDoc() {

            return totalDocs;
        }

        public static void LoadTotalDoc(Path path, Configuration conf) throws IOException {
            String namenodeURL = conf.get("fs.defaultFS");
            FileSystem fs = FileSystem.get(URI.create(namenodeURL), conf);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line = reader.readLine();
            totalDocs = Integer.parseInt(line);
        }

        public static void LoadCountTermInDoc(Path path, Configuration conf) throws IOException {
            String namenodeURL = conf.get("fs.defaultFS");
            FileSystem fs = FileSystem.get(URI.create(namenodeURL), conf);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                totalTermInDocMap.put(parts[0], Double.parseDouble(parts[1]));
            }
        }

        public static void LoadCountDocOfTerm(Path path, Configuration conf) throws IOException {
            String namenodeURL = conf.get("fs.defaultFS");
            FileSystem fs = FileSystem.get(URI.create(namenodeURL), conf);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                countDocOfTermMap.put(parts[0], Integer.parseInt(parts[1]));
            }
        }

        public static double countTermInDoc(String docid) throws IOException {
            if (totalTermInDocMap.get(docid) == null) {
                return 0.0;
            }
            return totalTermInDocMap.get(docid);
        }

        public static int countDocOfTerm(String termid) throws IOException {
            if (countDocOfTermMap.get(termid) == null) {
                return 0;
            }
            return countDocOfTermMap.get(termid);
        }
    }


    public class PairWritable implements WritableComparable<PairWritable> {
        private Text _termid;
        private Text _docid;
    
        public PairWritable(){
            set(new Text(String.valueOf(0)), new Text(String.valueOf(0)));
        }
        public PairWritable(Text termid, Text docid) {
            set(termid, docid);
        }
    
    
        public Text getTermid() {
            return _termid;
        }
    
        public Text getDocid() {
            return _docid;
        }
    
        public void set(Text termid, Text docid) {
            this._termid = termid;
            this._docid = docid;
        }
    
        @Override
        public void readFields(DataInput in) throws IOException {
            _termid.readFields(in);
            _docid.readFields(in);
        }
    
        @Override
        public void write(DataOutput out) throws IOException {
            _termid.write(out);
            _docid.write(out);
        }
    
        @Override
        public String toString() {
            return _termid + "," + _docid;
        }
    
        @Override
        public int compareTo(PairWritable tp) {
            int cmp = _termid.compareTo(tp._termid);
    
            if (cmp != 0) {
                return cmp;
            }
    
            return _docid.compareTo(tp._docid);
        }
    
        @Override
        public int hashCode(){
            return _termid.hashCode()*163 + _docid.hashCode();
        }
    
        @Override
        public boolean equals(Object o)
        {
            if(o instanceof PairWritable)
            {
                PairWritable tp = (PairWritable) o;
                return _termid.equals(tp._termid) && _docid.equals(tp._docid);
            }
            return false;
        }
    }




    public class TFIDFMapper extends Mapper<Object, Text, PairWritable, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.isEmpty()) {
                String[] tokens = line.split(" ");
                if (tokens.length == 3 && tokens[2].contains(".")) {
                    Text termid = new Text(tokens[0]);
                    Text docid = new Text(tokens[1]);
                    DoubleWritable frequency = new DoubleWritable(Double.parseDouble(tokens[2]));
                    PairWritable pair = new PairWritable(termid, docid);
                    context.write(pair, frequency);
                }
            }
        }
    }

    public class TFIDFReducer extends Reducer<PairWritable, DoubleWritable, PairWritable, DoubleWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String dataPath = conf.get("dataPath");
            Utils.LoadCountTermInDoc(new Path("/utils/totalTermInDoc.txt"), conf);
            Utils.LoadCountDocOfTerm(new Path("/utils/countDocOfTerm.txt"), conf);
            Utils.LoadTotalDoc(new Path("/utils/totalDoc.txt"), conf);
        }

        @Override
        protected void reduce(PairWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double docTotalTerms = Utils.countTermInDoc(key.getDocid().toString());
            int numDocsOfTerm = Utils.countDocOfTerm(key.getTermid().toString());
            int totalDoc = Utils.getTotalDoc();
            double tf, idf, tfidf;
            double termFrequency = 0;

            for (DoubleWritable val : values) {
                termFrequency += val.get();
            }

            tf = (double) termFrequency / docTotalTerms;
            idf = Math.log((double) totalDoc / (double) numDocsOfTerm);
            tfidf = tf * idf;

            DoubleWritable value = new DoubleWritable(tfidf);
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(Main.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setCombinerClass(TFIDFReducer.class);
        job.setReducerClass(TFIDFReducer.class);
        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.submit();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
