package com.atguigu.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * TODO
 *
 * @author TANPENG
 * @version 1.0
 * @date 2021/3/16 13:09
 */

public class WCDriver {

    public static void main(String[] args) throws Exception {

        Path inPath = new Path("D:/mrinput/wordcount");
        Path outPath = new Path("D:/mroutput/wordcount");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outPath)){
            fs.delete(outPath);
        }

        Job job = Job.getInstance(conf);

        job.setJobName("wordcount");

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);


    }
}
