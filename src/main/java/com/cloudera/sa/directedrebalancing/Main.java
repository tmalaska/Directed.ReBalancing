package com.cloudera.sa.directedrebalancing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class Main {
  
  public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  
  public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, ParseException {
    if (args.length == 0) {
      System.out.println("Directed Rebalancer:");
      System.out
          .println("Main {original file or directory} {numberOfMappers} {lowerLimitDate} {lowerLimitTime} {upperLimitDate} {upperLimitTime} {shouldDoDeletes}");
      System.out.println("Main foo/bar 10 2012-12-12 4:24 true");
      return;
    }

    String folder = args[0];
    int numberOfMappers = Integer.parseInt((args[1]));
    String lowerDateString = args[2];
    String lowerTimeString = args[3];
    String upperDateString = args[4];
    String upperTimeString = args[5];
    boolean shouldDelete = args[6].equals("true");
    
    long lowerBondTime = dateFormat.parse(lowerDateString + " " + lowerTimeString).getTime();
    long upperBondTime = dateFormat.parse(upperDateString + " " + upperTimeString).getTime();

    Configuration conf = new Configuration();
   
    // Create job
    Job job = new Job(new JobConf(conf));
    job.setJobName("ManyTxtToFewSeqJob");
    
    System.out.println("lowerBoundTimeStamp: " + lowerBondTime);
    System.out.println("upperBoundTimeStamp: " + upperBondTime);
    
    job.getConfiguration().setLong(ReBalancerMapper.LOWER_BOUND_TIME_STAMP_CONF, lowerBondTime);
    job.getConfiguration().setLong(ReBalancerMapper.UPPER_BOUND_TIME_STAMP_CONF, upperBondTime);
    job.getConfiguration().setBoolean(ReBalancerMapper.SHOULD_DELETE_ORIGINAL_FILE_CONF, shouldDelete);
    
    job.setJarByClass(Main.class);
    // Define input format and path
    job.setInputFormatClass(ReBalancerInputFormat.class);
    ReBalancerInputFormat.setInputPath(job, folder);
    ReBalancerInputFormat.setMapperNumber(job,
        numberOfMappers);

    // Define output format and path
    job.setOutputFormatClass(NullOutputFormat.class);
    
    // Define the mapper and reducer
    job.setMapperClass(ReBalancerMapper.class);
    // job.setReducerClass(Reducer.class);

    // Define the key and value format
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(0);

    // Exit
    job.waitForCompletion(true);
  }
}
