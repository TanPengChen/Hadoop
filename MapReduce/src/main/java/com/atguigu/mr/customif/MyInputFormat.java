package com.atguigu.mr.customif;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 1.改变切片策略，一个文件固定切1片，通过指定文件不可切
 *
 * 2.提供RR,这个RR读取切片的文件名作为Key,读取切片内容封装到bytes作为value
 */
public class MyInputFormat extends FileInputFormat{
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MyRecordReader();
    }

    //重写isSplitable
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
