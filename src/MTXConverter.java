package com.lab2.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.stream.IntStream;

public class MTXConverter {
    public static void main(String[] args) {
        if(args.length < 2){
            System.out.println("Usage: <input path>, <output path>");
            return;
        }
        Path outputPath = new Path(args[0]);
        Configuration conf = new Configuration();
        final String comment = "%%MatrixMarket matrix coordinate real general";
        try{
            Job job = new Job(conf);
            String namenodeUrl = job.getConfiguration().get("fs.defaultFS");
            FileSystem fs = FileSystem.get(URI.create(namenodeUrl), conf);
            FSDataInputStream inputStream = fs.open(outputPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            int[] maxs = new int[3];

            // Read the file line by line
            ArrayList<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                // Process each line of the file
//                    System.out.println(line);
                String[] tokens = line.split("[,\t ]");
                lines.add(String.join(" ", tokens));
                IntStream.range(0, 3).forEach(i -> maxs[i] = Math.max(maxs[i], Integer.parseInt(tokens[i])));
            }
            String[] header = new String[3];
            for(int i = 0; i < header.length; i++){
                header[i] = String.valueOf(maxs[i]);
            }
            String headerLine = String.join(" ", header);

            // Close the readers
            reader.close();
            inputStream.close();


            // Write the file to HDFS
            FSDataOutputStream outputStream = fs.create(new Path(args[1]));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

            writer.write(comment);
            writer.newLine();
            writer.write(headerLine);
            writer.newLine();
            for (String l : lines) {
                writer.write(l);
                writer.newLine();
            }

            writer.close();
            outputStream.close();


        }
        catch (NullPointerException ne){
            String namenodeUrl = conf.get("fs.defaultFS");
            System.err.println(namenodeUrl);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}
