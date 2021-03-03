package com.seq;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
 import org.apache.hadoop.util.ReflectionUtils;
 public class SequenceFileReader {  
   public static void main(String[] args) throws IOException {
	   Configuration conf = new Configuration();
       conf.set("fs.defaultFS", "file:///");

       FileSystem fs = FileSystem.get(conf);

       Path path = new Path("/home/hadoop01/test.seq");

       SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

       Text key = new Text();
       BytesWritable value = new BytesWritable();

       while ((reader.next(key, value))) {
           long position = reader.getPosition();
           System.out.println("key: " + key.toString() + " , " + " val: " + value.get() + " , " + " pos: " + position);
       }
   }
 }
     
 