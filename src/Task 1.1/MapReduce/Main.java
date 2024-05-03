import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length < 3){
            System.out.println("<datapath> <input path> <output path>");
        }
        Job job = Job.getInstance(new Configuration());
//        Configuration conf = job.getConfiguration();
        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(Main.class);
        job.setMapperClass(MapperOne.class);
        job.setCombinerClass(ReducerOne.class);
        job.setReducerClass(ReducerOne.class);
        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(IntWritable.class);
        String dataPath = args[0];
        job.getConfiguration().set("dataPath", dataPath);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.submit();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
