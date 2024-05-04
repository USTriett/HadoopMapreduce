import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class KMeansTFIDF {
    public static class CentroidUtils {
        public static Map<Integer, double[]> loadCentroids(Configuration conf) {
            
            int numCentroids = 5; // Number of centroids
            int dimensions = 2; // Number of dimensions, adjust this based on your actual data needs
            Map<Integer, double[]> centroids = new HashMap<>();
            Random rand = new Random();
        
            // Initialize centroids with random values
            for (int i = 0; i < numCentroids; i++) {
                double[] centroid = new double[dimensions];
                for (int j = 0; j < dimensions; j++) {
                    // Initialize each dimension of the centroid with a random value
                    // You might want to adjust these ranges depending on the expected range of your data
                    centroid[j] = rand.nextDouble();
                }
                centroids.put(i, centroid);
            }
            return centroids;
        }
    
        public static int findClosestCentroid(double[] vector, Map<Integer, double[]> centroids) {
            double maxCosine = -1;
            int closestCentroidIndex = -1;
            for (Map.Entry<Integer, double[]> entry : centroids.entrySet()) {
                double cosine = computeCosineSimilarity(vector, entry.getValue());
                if (cosine > maxCosine) {
                    maxCosine = cosine;
                    closestCentroidIndex = entry.getKey();
                }
            }
            return closestCentroidIndex;
        }
    
        public static double computeCosineSimilarity(double[] vectorA, double[] vectorB) {
            double dotProduct = 0;
            double normA = 0;
            double normB = 0;
            for (int i = 0; i < vectorA.length; i++) {
                dotProduct += vectorA[i] * vectorB[i];
                normA += Math.pow(vectorA[i], 2);
                normB += Math.pow(vectorB[i], 2);
            }
            return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
        }
    
        public static double[] calculateNewCentroid(List<double[]> vectors) {
            double[] centroid = new double[vectors.get(0).length];
            for (double[] vec : vectors) {
                for (int i = 0; i < vec.length; i++) {
                    centroid[i] += vec[i];
                }
            }
            for (int i = 0; i < centroid.length; i++) {
                centroid[i] /= vectors.size();
            }
            return centroid;
        }
    
        public static String centroidToString(double[] centroid) {
            return Arrays.toString(centroid);
        }
    }

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Map<Integer, double[]> centroids;
    
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            centroids = CentroidUtils.loadCentroids(context.getConfiguration());
        }
    
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            int docid = Integer.parseInt(tokens[1]);
            int termid = Integer.parseInt(tokens[0]);
            double tfidf = Double.parseDouble(tokens[2]);
    
            double[] vector = new double[]{termid, tfidf};  // Simplified representation
            int closestCentroid = CentroidUtils.findClosestCentroid(vector, centroids);
    
            context.write(new IntWritable(closestCentroid), value);
        }
    }
    
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<double[]> termVectors = new ArrayList<>();
            for (Text value : values) {
                String[] parts = value.toString().split("\\s+");
                double tfidf = Double.parseDouble(parts[2]);
                termVectors.add(new double[]{Double.parseDouble(parts[0]), tfidf});
            }
    
            double[] newCentroid = CentroidUtils.calculateNewCentroid(termVectors);
            context.write(key, new Text(CentroidUtils.centroidToString(newCentroid)));
        }
    }
    


    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int iterations = 10;

        Configuration conf = new Configuration();

        for (int i = 0; i < iterations; i++) {
            conf.setInt("iteration", i);
            Job job = Job.getInstance(conf, "K-Means Clustering");
            job.setJarByClass(KMeansTFIDF.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath, "iteration" + i));

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
    }
}
