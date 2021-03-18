package com.atguigu.mr.customif;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CustomIFDriver {
    public static void main(String[] args) throws Exception {

//        Path inputPath = new Path("D:/mrinput/wordcount");
//        Path outputPath = new Path("D:/mroutput/wordcount");
        Path inputPath = new Path("D:/mrinput/custom");
        Path outputPath = new Path("D:/mroutput/custom");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        //创建job
        Job job = Job.getInstance(conf);
        //告诉NM运行时，MR中Job所在的Jar包位置
        //job.setJar("MapReduce-1.0-SNAPSHOT");
//        job.setJarByClass(CustomIFDriver.class);
        //为Job创建一个名字
        job.setJobName("wordcount");
        //设置job
            //设置job运行的maooer reducer类型 Mapper,reducer输出的key-value类型
            job.setMapperClass(CustomIFMapper.class);
            job.setReducerClass(CustomIFReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);

            FileInputFormat.setInputPaths(job,inputPath);
            FileOutputFormat.setOutputPath(job,outputPath);

            //设置输入输出格式
            job.setInputFormatClass(MyInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //运行job
        job.waitForCompletion(true);
    }
}
