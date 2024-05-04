
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
import java.util.*;
import java.text.DecimalFormat;



import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;


public class Task2_2 {

    
    public static class CentroidUtils {
        public static Map<Integer, DataPoint[]> centroids = new HashMap<>();

        public static int findClosestCentroid(DataPoint vector, Map<Integer, DataPoint[]> centroids) {
            double maxCosine = -1;
            int closestCentroidIndex = -1;
            for (Map.Entry<Integer, DataPoint[]> entry : centroids.entrySet()) {
                // double cosine = computeCosineSimilarity(vector, entry.getValue());
                double cosine = entry.getValue().cosineSimilarity(vector);
                if (cosine > maxCosine) {
                    maxCosine = cosine;
                    closestCentroidIndex = entry.getKey();
                }
            }
            return closestCentroidIndex;
        }

        public static void loadCentroids(Path path, Configuration conf) throws IOException {
            String namenodeURL = conf.get("fs.defaultFS");
            FileSystem fs = FileSystem.get(URI.create(namenodeURL), conf);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            line = reader.readLine();
            line = reader.readLine();

            int numCentroids = 5; // Number of centroids
            DataPoint[] centroid = new DataPoint[numCentroids];
            for (int i = 0; i < numCentroids; i++) {
                line = reader.readLine();
                centroid[i] = new DataPoint(line);
                centroids.put(i, centroid[i]);
            }

            
        }

        // public static double computeCosineSimilarity(double[] vectorA, double[] vectorB) {
        //     double dotProduct = 0;
        //     double normA = 0;
        //     double normB = 0;
        //     for (int i = 0; i < vectorA.length; i++) {
        //         dotProduct += vectorA[i] * vectorB[i];
        //         normA += Math.pow(vectorA[i], 2);
        //         normB += Math.pow(vectorB[i], 2);
        //     }
        //     return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
        // }
    }

    class DataPoint {
        int docId;
        Map<Integer, Double> tfidf;

        DataPoint(String line) {
            String[] parts = line.split("\\|");
            this.docId = Integer.parseInt(parts[0]);
            this.tfidf = new HashMap<>();
            String[] features = parts[1].split(",");
            for (String feature : features) {
                String[] featureParts = feature.split(":");
                int termId = Integer.parseInt(featureParts[0]);
                double value = Double.parseDouble(featureParts[1]);
                tfidf.put(termId, value);
            }
        }

        double cosineSimilarity(DataPoint other) {
            double dotProduct = 0;
            double norm1 = 0, norm2 = 0;
            for (Map.Entry<Integer, Double> entry : tfidf.entrySet()) {
                int termId = entry.getKey();
                double value1 = entry.getValue();
                double value2 = other.tfidf.getOrDefault(termId, 0.0);
                dotProduct += value1 * value2;
                norm1 += value1 * value1;
                norm2 += value2 * value2;
            }
            return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
        }
    }

    public static class CentroidInitializer {
        

        // private class DataPoint {
        //     int docId;
        //     Map<Integer, Double> tfidf;
    
        //     DataPoint(String line) {
        //         String[] parts = line.split("\\|");
        //         this.docId = Integer.parseInt(parts[0]);
        //         this.tfidf = new HashMap<>();
        //         String[] features = parts[1].split(",");
        //         for (String feature : features) {
        //             String[] featureParts = feature.split(":");
        //             int termId = Integer.parseInt(featureParts[0]);
        //             double value = Double.parseDouble(featureParts[1]);
        //             tfidf.put(termId, value);
        //         }
        //     }
    
        //     double cosineSimilarity(DataPoint other) {
        //         double dotProduct = 0;
        //         double norm1 = 0, norm2 = 0;
        //         for (Map.Entry<Integer, Double> entry : tfidf.entrySet()) {
        //             int termId = entry.getKey();
        //             double value1 = entry.getValue();
        //             double value2 = other.tfidf.getOrDefault(termId, 0.0);
        //             dotProduct += value1 * value2;
        //             norm1 += value1 * value1;
        //             norm2 += value2 * value2;
        //         }
        //         return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
        //     }
        // }
        private int k; // number of clusters
        private List<DataPoint> data; // input data points
        private List<DataPoint> centroids; // cluster centroids
    
        public CentroidInitializer(int k, List<DataPoint> data) {
            this.k = k;
            this.data = data;
            this.centroids = new ArrayList<>();
        }
    
        public List<DataPoint> getCentroids() {
            initCentroids();
            return centroids;
        }
    
        private void initCentroids() {
            Random rand = new Random();
    
            // Choose the first centroid randomly
            centroids.add(data.get(rand.nextInt(data.size())));
    
            // Choose the remaining centroids using the K-Means|| algorithm
            for (int i = 1; i < k; i++) {
                DataPoint newCentroid = chooseCentroidParallel();
                centroids.add(newCentroid);
            }
        }

        
    
        public static Map<Integer, DataPoint[]> loadCentroids(Configuration conf) {


                
            int numCentroids = 5; // Number of centroids
            int dimensions = conf.get("Number of terms"); // Number of dimensions, adjust this based on your actual data needs
            Map<Integer, DataPoint[]> centroids = new HashMap<>();
            Random rand = new Random();
        
            // Initialize centroids with random values
            for (int i = 0; i < numCentroids; i++) {
                DataPoint[] centroid = new DataPoint[dimensions];
                
                centroids.put(i, centroid);
            }
            return centroids;
        }
                
    
        private DataPoint chooseCentroidParallel() {
            double maxDist = 0;
            int maxIndex = 0;
    
            for (int i = 0; i < data.size(); i++) {
                double minDist = Double.MAX_VALUE;
                for (DataPoint centroid : centroids) {
                    double dist = data.get(i).cosineSimilarity(centroid);
                    if (dist < minDist) {
                        minDist = dist;
                    }
                }
                if (minDist > maxDist) {
                    maxDist = minDist;
                    maxIndex = i;
                }
            }
    
            return data.get(maxIndex);
        }
    
    
    }



    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Map<Integer, DataPoint[]> centroids;
    
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            loadCentroids(context.get("dataPath"));
            centroids = CentroidUtils.centroids;
        }
    
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.split("\\|");
            int docId = Integer.parseInt(parts[0]);
            Map<Integer, Double> tfidf = new HashMap<>();
            String[] features = parts[1].split(",");
            for (String feature : features) {
                String[] featureParts = feature.split(":");
                int termId = Integer.parseInt(featureParts[0]);
                double val = Double.parseDouble(featureParts[1]);
                tfidf.put(termId, val);
            }

            DataPoint vector = new DataPoint(value.toString());
            int closestCentroid = CentroidUtils.findClosestCentroid(vector, centroids);
    
            context.write(new IntWritable(closestCentroid), new Text(parts[1]));
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ans = "";
            for (Text value : values) {
                ans += value.toString() + "\t";
            }
            context.write(key, new Text(ans));
        }
    }

    


    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("<datapath> <input path> <output path>");
        }

        Job job = Job.getInstance(new Configuration());
        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(Task2_2.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansReducer.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
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
