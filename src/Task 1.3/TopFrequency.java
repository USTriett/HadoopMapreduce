import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopFrequency {

    private static String dataPath;

    public static class MatrixTopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Double, Text> topTenMap = new TreeMap<>();
        private Map<Integer, String> termMap = new HashMap<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
    	// Retrieve dataPath from configuration
	    Configuration conf = context.getConfiguration();
            String dataPath = conf.get("dataPath");	
	    String namenodeURL = conf.get("fs.defaultFS");
            FileSystem fs = FileSystem.get(URI.create(namenodeURL), conf);
    	    Path path = new Path(dataPath + "/bbc.terms");
	    FSDataInputStream inputStream = fs.open(new Path(path.toString()));
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	    String line;
	    int termId = 1;
	    while ((line = reader.readLine()) != null) {
	        termMap.put(termId++, line.trim());
	    }
	    reader.close();
	}


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");

            if (tokens.length != 3)
                return; // Malformed line, skip

            int termId = Integer.parseInt(tokens[0]);
            int docId = Integer.parseInt(tokens[1]);
            double frequency = Double.parseDouble(tokens[2]);

            String term = termMap.get(termId);
            if (term != null) {
                Text outputValue = new Text(term + " " + frequency);

                topTenMap.put(frequency, outputValue);

                if (topTenMap.size() > 10) {
                    topTenMap.remove(topTenMap.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
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
                String[] components = value.toString().split("\\s+");
                if (components.length != 2)
                    continue; // Malformed line, skip

                double frequency = Double.parseDouble(components[1]);

                Text outputValue = new Text(components[0] + " " + frequency);

                topTenMap.put(frequency, outputValue);

                if (topTenMap.size() > 10) {
                    topTenMap.remove(topTenMap.firstKey());
                }
            }

            for (Text value : topTenMap.descendingMap().values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

	public static void main(String[] args) throws Exception {

 		if(args.length < 3){
            		System.out.println("<datapathToBBCTerm> <input path> <output path>");
       		}
        	Job job = Job.getInstance(new Configuration());
		Configuration conf = job.getConfiguration();
   		job.setJarByClass(TopFrequency.class);
    		job.setMapperClass(MatrixTopTenMapper.class);
    		job.setReducerClass(MatrixTopTenReducer.class);
    		job.setNumReduceTasks(1);
    		job.setOutputKeyClass(NullWritable.class);
    		job.setOutputValueClass(Text.class);

        	String dataPath = args[0];
        	job.getConfiguration().set("dataPath", dataPath);
        	FileInputFormat.addInputPath(job, new Path(args[1]));
        	FileInputFormat.setInputDirRecursive(job, true);
        	FileOutputFormat.setOutputPath(job, new Path(args[2]));

        	job.submit();

        	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
