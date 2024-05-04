import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class KMeans {

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {

        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] centroidStrings = conf.getStrings("centroids");
            for (String centroidString : centroidStrings){
                String[] a = centroidString.split(" ");
                centroids.add(new double[]{Double.parseDouble(a[0]), Double.parseDouble(a[1])});
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse data point
            String[] parts = value.toString().split("\\s+");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);

            // Find closest centroid
            int closestCentroidIdx = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double[] centroid = centroids.get(i);
                double distance = Math.sqrt(Math.pow(x - centroid[0], 2) + Math.pow(y - centroid[1], 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidIdx = i;
                }
            }

            // Emit data point with the closest centroid's index as the key
            context.write(new Text(String.valueOf(closestCentroidIdx)), value);
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Collect data points assigned to this centroid
            List<double[]> dataPoints = new ArrayList<>();
            for (Text value : values) {
                String[] parts = value.toString().split("\\s+");
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                dataPoints.add(new double[]{x, y});
            }

            // Calculate new centroid as the mean of data points
            double sumX = 0, sumY = 0;
            StringBuilder sb = new StringBuilder();
            for (double[] point : dataPoints) {
                sumX += point[0];
                sumY += point[1];
                sb.append(point[0] +" "+ point[1]);
            }
            double meanX = sumX / dataPoints.size();
            double meanY = sumY / dataPoints.size();

            Text outputKey = new Text(meanX + " " + meanY);
            context.write(outputKey, new Text());
            Configuration conf = context.getConfiguration();
            mos.write(outputKey, new Text(sb.toString()),conf.get("outputPath")+"_clusters");
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int k = 3;
        int numIterations = 20;

        // Initial centroids setup
        setupInitialCentroids(k, conf);

        for (int i = 0; i < numIterations; i++) {
            String outputString = args[1] + "_" + (i+1);
            conf.set("outputPath", outputString);

            Job job = Job.getInstance(conf, "KMeans Iteration " + (i+1));
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));

            FileOutputFormat.setOutputPath(job, new Path(outputString));
            job.waitForCompletion(true);

            updateCentroids(conf, outputString);
        }
    }

    private static void setupInitialCentroids(int k, Configuration conf) {
        // Initialize centroids randomly only for the first time
        Random random = new Random();
        List<String> centroids = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            double centroidX = random.nextDouble() * 100;
            double centroidY = random.nextDouble() * 100;
            centroids.add(centroidX + " " + centroidY);
        }
        conf.setStrings("centroids", centroids.toArray(new String[0]));
    }

    private static void updateCentroids(Configuration conf, String outputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path iterationOutputPath = new Path(outputPath);
        FileStatus[] status = fs.listStatus(iterationOutputPath);
        List<String> newCentroids = new ArrayList<>();
        for (FileStatus file : status) {
            if(file.getPath().getName().startsWith("_")) // Ignore system-generated files of Hadoop
                continue;

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                newCentroids.add(line); // Assuming the format is centroidIndex\tcentroidX centroidY
            }
            br.close();
        }
        conf.setStrings("centroids", newCentroids.toArray(new String[0]));
    }
}

