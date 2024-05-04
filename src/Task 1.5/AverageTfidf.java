import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.util.Map;
import java.util.TreeMap;


public class AverageTFIDF {

	public static class TFIDFMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	    private IntWritable classId = new IntWritable();
	    private Text termTFIDF = new Text();
	
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        if (line.startsWith("%%MatrixMarket")) {
	            return; // Skip the header line
	        }
	        String[] tokens = line.split("\\s+");
	        if (tokens.length >= 3) {
	            int termId = Integer.parseInt(tokens[0]);
	            int docId = Integer.parseInt(tokens[1]);
	            double tfidf = Double.parseDouble(tokens[2]);
	            classId.set(docId); // Assuming docId is the class ID
	            termTFIDF.set(termId + " " + tfidf);
	            context.write(classId, termTFIDF);
	        }
	    }
	}


	public static class TFIDFReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	    private TreeMap<Double, String> topTerms = new TreeMap<>();
	    private IntWritable classId = new IntWritable();
	
	    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        int count = 0;
	        double sum = 0.0;
	        
	        for (Text val : values) {
	            String[] tokens = val.toString().split("\\s+");
	            double tfidf = Double.parseDouble(tokens[1]);
	            sum += tfidf;
	            count++;
	        }
	
	        double avgTFIDF = sum / count;
	        classId.set(key.get());
	        topTerms.put(avgTFIDF, key.toString() + " " + avgTFIDF);
	
	        // Keep only top 5 terms
	        if (topTerms.size() > 5) {
	            topTerms.remove(topTerms.firstKey());
	        }
	    }
	
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        for (Map.Entry<Double, String> entry : topTerms.descendingMap().entrySet()) {
	            context.write(classId, new Text(entry.getValue()));
	        }
	    }
	}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TFIDF Class Average");
        job.setJarByClass(AverageTFIDF	.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
