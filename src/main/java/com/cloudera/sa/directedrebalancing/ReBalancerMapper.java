package com.cloudera.sa.directedrebalancing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReBalancerMapper extends
    Mapper<LongWritable, Text, NullWritable, NullWritable> {

  Text newKey = new Text();
  Text newValue = new Text();
  FileSystem hdfs;

  public static String LOWER_BOUND_TIME_STAMP_CONF = "lower.bound.time.stamp";
  public static String UPPER_BOUND_TIME_STAMP_CONF = "upper.bound.time.stamp";
  public static String POSTFIX_OF_NEW_FILE_CONF = "postfix.of.new.file";
  public static String POSTFIX_OF_EXISTING_FILE_CONF = "postfix.of.existing.file";
  public static String SHOULD_DELETE_ORIGINAL_FILE_CONF = "should.delete.original.file";
  
  
  Long lowerBoundTimeStamp;
  Long upperBoundTimeStamp;
  String postFixOfNewFile;
  String postFixOfExistingFile;
  boolean shouldDeleteOriginalFile;
  

  @Override
  public void setup(Context context) throws IOException {
    
    hdfs = FileSystem.get(context.getConfiguration());

    lowerBoundTimeStamp = context.getConfiguration().getLong(LOWER_BOUND_TIME_STAMP_CONF, 0);
    upperBoundTimeStamp = context.getConfiguration().getLong(UPPER_BOUND_TIME_STAMP_CONF, Long.MAX_VALUE);
    postFixOfNewFile = context.getConfiguration().get(POSTFIX_OF_NEW_FILE_CONF, "copy.tmp");
    postFixOfExistingFile = context.getConfiguration().get(POSTFIX_OF_EXISTING_FILE_CONF, "orig.tmp");
    shouldDeleteOriginalFile = context.getConfiguration().getBoolean(SHOULD_DELETE_ORIGINAL_FILE_CONF, false);
    
    System.out.println("lowerBoundTimeStamp: " + lowerBoundTimeStamp);
    System.out.println("upperBoundTimeStamp: " + upperBoundTimeStamp);
    
  }

  @Override
  public void cleanup(Context context) throws IOException {
    hdfs.close();
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    processSingleFile(new Path(value.toString()), context);
  }

  private void processSingleFile(Path sourceFilePath, Context context)
      throws IOException, InterruptedException {

    context.getCounter("Files", "Files.Started").increment(1);
    FileStatus originalFileStatus = hdfs.getFileStatus(sourceFilePath);
    
    System.out.println("Skiping: " + originalFileStatus.getModificationTime() + " : " + lowerBoundTimeStamp + "," + upperBoundTimeStamp);
    System.out.println("-Skiping: " + new Date(originalFileStatus.getModificationTime()) + " : " + new Date(lowerBoundTimeStamp) + "," + new Date(upperBoundTimeStamp));
    
    
    //Is the file with in our time range?
    if (!(originalFileStatus.getModificationTime() >= lowerBoundTimeStamp &&
        originalFileStatus.getModificationTime() <= upperBoundTimeStamp)) {
      
      
      context.getCounter("Files", "Files.Skiped").increment(1);  
      context.getCounter("Files", "Files.Finished").increment(1);
      return;
    }
    
    context.getCounter("Files", "Total.File.Bytes").increment(
        originalFileStatus.getLen());
    context.getCounter("Files", "Total.Num.Of.Blocks").increment(
        originalFileStatus.getLen() / originalFileStatus.getBlockSize());

    BufferedReader reader = new BufferedReader(new InputStreamReader(
        hdfs.open(sourceFilePath)));

    
    Path copiedFilePath = new Path(sourceFilePath + "." + postFixOfNewFile);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(copiedFilePath)));
    
    try {
      char[] cBuf = new char[10000];  
      int bytesRead = 0;
      
      while ((bytesRead = reader.read(cBuf)) > 0) {

        context.getCounter("Files", "Bytes.Writen").increment(bytesRead);

        writer.write(cBuf, 0, bytesRead);
      }
    } finally {
      reader.close();
      writer.close();
    }
    
    FileStatus copiedFileStatus = hdfs.getFileStatus(copiedFilePath);
    
    if (copiedFileStatus.getLen() == originalFileStatus.getLen()){
      Path originalMovePath = new Path(sourceFilePath + "." + postFixOfExistingFile);
      if (hdfs.rename(sourceFilePath, originalMovePath)) {
        if (hdfs.rename(copiedFilePath, sourceFilePath)) {
          System.out.println("Successful copy of " + sourceFilePath);
          if (shouldDeleteOriginalFile) {
            hdfs.delete(originalMovePath, false);
          }
        } else {
          System.err.println("Failed to rename " + copiedFilePath + " to " + sourceFilePath);
          context.getCounter("Files", "Files.Rename.Issue").increment(1);
          return;
        }
      } else {
        System.err.println("Failed to rename " + sourceFilePath);
        context.getCounter("Files", "Files.Rename.Issue").increment(1);
        return;
      }
    } else {
      System.err.println(copiedFileStatus + " is not like " + originalFileStatus);
      context.getCounter("Files", "Mismatch.File.Size").increment(1);
      return;
    }
    
    
    context.getCounter("Files", "Files.Finished").increment(1);
  }

}
