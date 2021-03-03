package com.seq;

import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileTest {

//输出文件夹
 static String PATH = "/home/hadoop01/output/test.seq";
 static SequenceFile.Writer writer = null;
 public static void main(String[] args) throws Exception{

	 
  Configuration conf = new Configuration();
  conf.set("fs.default.name", "file:///");
  conf.set("mapred.job.tracker", "local");
 
 
 
//输入文件夹
  String path = "/home/hadoop01/test/";
  URI uri = new URI(path);
  FileSystem fileSystem = FileSystem.get(uri, conf);

  writer = SequenceFile.createWriter(fileSystem, conf, new Path(PATH), Text.class, BytesWritable.class);
  

  listFileAndWriteToSequenceFile(fileSystem,path);
  System.out.println("Seq文件路径"+PATH+"----文件名："+"test.seq");
  org.apache.hadoop.io.IOUtils.closeStream(writer);
 
}
 
 
 
//遍历写入seq文件
 public static void listFileAndWriteToSequenceFile(FileSystem fileSystem,String path) throws Exception{
  final FileStatus[] listStatuses = fileSystem.listStatus(new Path(path));
  for (FileStatus fileStatus : listStatuses) {
   if(fileStatus.isFile()){
    Text fileText = new Text(fileStatus.getPath().toString());
    System.out.println("文件读取路径：");
    System.out.println(fileText.toString());
    FSDataInputStream in = fileSystem.open(new Path(fileText.toString()));
    byte[] buffer = IOUtils.toByteArray(in);
    in.read(buffer);
    BytesWritable value = new BytesWritable(buffer);
    writer.append(fileText, value);

   }
   if(fileStatus.isDirectory()){
    listFileAndWriteToSequenceFile(fileSystem,fileStatus.getPath().toString());
   }
//   org.apache.hadoop.io.IOUtils.closeStream(writer);
   
  }
 }}
