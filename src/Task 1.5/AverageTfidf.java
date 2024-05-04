import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class AverageTFIDF {

    public static class AverageTFIDFMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String termID = parts[0].split(" ")[0];
            String classID = parts[0].split(" ")[1];
            double tfidf = Double.parseDouble(parts[1]);

            context.write(new Text(classID), new Text(termID + " " + tfidf));
        }
    }

    public static class AverageTFIDFReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> termTFIDFMap = new HashMap<>();

            for (Text value : values) {
                String[] parts = value.toString().split(" ");
                String termID = parts[0];
                double tfidf = Double.parseDouble(parts[1]);

                termTFIDFMap.put(termID, termTFIDFMap.getOrDefault(termID, 0.0) + tfidf);
            }

            TreeMap<Double, String> sortedTFIDF = new TreeMap<>();
            for (Map.Entry<String, Double> entry : termTFIDFMap.entrySet()) {
                String termID = entry.getKey();
                double tfidfSum = entry.getValue();
                double avgTFIDF = tfidfSum / termTFIDFMap.size();
                sortedTFIDF.put(avgTFIDF, termID);
                if (sortedTFIDF.size() > 5) {
                    sortedTFIDF.remove(sortedTFIDF.firstKey());
                }
            }

            StringBuilder result = new StringBuilder();
            for (Map.Entry<Double, String> entry : sortedTFIDF.descendingMap().entrySet()) {
                result.insert(0, entry.getValue() + " " + entry.getKey() + " ");
            }
            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 1");

        job.setJarByClass(AverageTFIDF.class);
        job.setMapperClass(AverageTFIDFMapper.class);
        job.setReducerClass(AverageTFIDFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        Path resultFile = new Path("Task_1_5.txt");

        try (FSDataOutputStream outputStream = fs.create(resultFile)) {
            for (FileStatus status : fs.listStatus(outputPath)) {
                if (status.isFile() && status.getPath().getName().startsWith("part")) {
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            outputStream.writeBytes(line + "\n");
                        }
                    }
                }
            }
        }

        fs.delete(outputPath, true);
    }
}
