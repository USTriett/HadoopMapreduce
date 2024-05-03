import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

public class Utils {

//    public static String dataDirectory = "";

    public static class Resource{
        private static ArrayList<String> stopWords = new ArrayList<>();
        private static HashMap<String, String> idsMap = new HashMap<>();
        public static ArrayList<String> getStopWords(){

            return stopWords;
        }

        public static void LoadStopWords(Path path, Configuration conf) {
            try{
                String namenodeUrl = conf.get("fs.defaultFS");
                FileSystem fs = FileSystem.get(URI.create(namenodeUrl), conf);
                FSDataInputStream inputStream = fs.open(new Path(path.toString()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                // Read the file line by line
                String line;
                while ((line = reader.readLine()) != null) {
                    // Process each line of the file
                    stopWords.add(line);
                }

                // Close the readers
                reader.close();
                inputStream.close();

            }
            catch (NullPointerException ne){
                String namenodeUrl = conf.get("fs.defaultFS");
                System.err.println(namenodeUrl);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public static void LoadIdsMap(Path path, Configuration conf) throws IOException {
            String namenodeURL = conf.get("fs.defaultFS");
            FileSystem fs = FileSystem.get(URI.create(namenodeURL), conf);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            int count = 1;
            while((line = reader.readLine()) != null){
                idsMap.put(line, String.valueOf(count));

                count++;
            }

        }

        public static Text getTermid(String token) throws IOException {
            if(idsMap.get(token) == null){
                System.out.println(token);
                return  new Text(String.valueOf(0));
            }
            return new Text(idsMap.get(token));

        }



        public static Text getDocid(String docid){
            if(idsMap.get(docid) == null){
                return new Text(String.valueOf(0));
            }
            return new Text(idsMap.get(docid));
        }


    }
}
