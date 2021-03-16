package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TODO
 *
 * @author TANPENG
 * @version 1.0
 * @date 2021/3/16 13:00
 */

public class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private Text key_in = new Text();

    private IntWritable out_value = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println("key_in:" + key + "out_value：" + out_value);

        String[] words = value.toString().split("\t");
        for (String word:words) {
            key_in.set(word);
            context.write(key_in,out_value);
        }
    }
}
