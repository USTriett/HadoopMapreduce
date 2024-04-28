package com.lab2.partone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerOne extends Reducer<PairWritable, IntWritable, PairWritable, IntWritable> {
    @Override
    protected void reduce(PairWritable key, Iterable<IntWritable> values, Reducer<PairWritable, IntWritable, PairWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value : values){
            sum += value.get();
        }


        context.write(key, new IntWritable(sum));
    }
}
