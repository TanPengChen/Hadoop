package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * TODO
 *
 * @author TANPENG
 * @version 1.0
 * @date 2021/3/16 13:05
 */

public class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable out_value = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable intWritable:values) {
            sum+=intWritable.get();
        }
        out_value.set(sum);
        context.write(key,out_value);
    }
}
