
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class MapperOne extends Mapper<Object, Text, PairWritable, IntWritable> {
    private IntWritable one = new IntWritable(1);

    @Override
    protected void setup(Mapper<Object, Text, PairWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String dataPath = conf.get("dataPath");
        Utils.Resource.LoadIdsMap(new Path(dataPath + "/bbc.terms"), conf);
        Utils.Resource.LoadIdsMap(new Path(dataPath + "/bbc.docs"), conf);
        Utils.Resource.LoadStopWords(new Path(dataPath + "/stopwords.txt"), conf);


    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, PairWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        ArrayList<String> stopWords = Utils.Resource.getStopWords();
        String normalize = value.toString().toLowerCase().replaceAll("[\\n\\t]", "");
        String[] tokens = normalize.split("[^a-zA-Z0-9£]+");
        for(String token : tokens){
            if(stopWords.contains(token)){
                continue;
            }

            FileSplit split = (FileSplit) context.getInputSplit();
            String filePath = split.getPath().toString();
            String[] parts = filePath.split("/");
            String[] name = split.getPath().getName().split("\\.");
            String docid = parts[parts.length - 2] + "." + name[0];
//            System.out.println(docid);
//            Configuration configuration = context.getConfiguration();
//            Path path = new Path(configuration.get("dataPath"));
            PairWritable pair = new PairWritable(Utils.Resource.getTermid(token), Utils.Resource.getDocid(docid));
            if(Objects.equals(pair.getdocid(), "0") || Objects.equals(pair.getTermid(), "0"))
                continue;
            context.write(pair, one);
        }
    }
}
