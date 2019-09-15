/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.operations.dendis.DENDIS;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Closest pair of points algorithm
 * @author Ahmed Eldawy
 *
 */
public class DENDISSampler {

  /**Logger to write log messages for this class*/
  static final Log LOG = LogFactory.getLog(DENDISSampler.class);

  /**
   * The map function computes the closest pair for a partition and returns all
   * points that can possibly contribute to the global closest pair. This
   * includes the closest pair found in this partition as well as all points
   * that are closer to the partition boudnary than the distance between the
   * closest pair.
   * @author Ahmed Eldawy
   *
   */
  public static class DENDISSamplerMap
      extends Mapper<Rectangle, Iterable<Point>, NullWritable, Point> {

    final NullWritable dummy = NullWritable.get();
    Point outputPoint = new Point();
    double sampleRatio;
    int maxShapeOneTime;
    double epsilon = 0.01;//0.0001;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      sampleRatio = context.getConfiguration().getDouble("ratio", 0.01);
      maxShapeOneTime = context.getConfiguration().getInt("max", 10000);
      epsilon = context.getConfiguration().getDouble("epsilon", 0.01);
    }

    @Override
    protected void map(Rectangle key, Iterable<Point> values, Context context)
        throws IOException, InterruptedException {

      List<Point> points = new ArrayList<Point>();
      int size = 0;
      while(values.iterator().hasNext()){
        Point shape = values.iterator().next();
        points.add(shape.clone());
        if(points.size()==maxShapeOneTime||!points.iterator().hasNext()){
          int N_instance = (int)(points.size()*sampleRatio);//En param�tre un nombre d'instance MAX

          double[][] data = new double[points.size()][2];
          for(int n = 0; n < points.size(); n++){
            Point point = points.get(n);
            data[n][0] = point.x;
            data[n][1] = point.y;
          }
          size += points.size();
          points.clear();
          System.gc();

          DENDIS sampler = new DENDIS();


          int[] weight = new int[N_instance];

          LOG.info(data.length+":"+2+":"+epsilon+":"+N_instance+":"+weight.length);
          LOG.info(Runtime.getRuntime().freeMemory()/1024/1024);
          LOG.info(Runtime.getRuntime().maxMemory()/1024/1024);
          double[][] prototype = sampler.SamplingApproachP_DENDIS(data,data.length,2,epsilon,N_instance,weight,1);
          for(double[] proto : prototype){
            outputPoint.x = proto[0];
            outputPoint.y = proto[1];
            context.write(dummy,outputPoint);
          }
        }

      }
      LOG.info("TOTAL SIZE: "+size);
    }
  }

  public static <T extends Shape> Job dendisSamplerMapReduce(Path[] inPaths, Path out,
                                           ResultCollector<T> output,
                                           OperationsParams params)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(params, "Closest Pair");
    job.setJarByClass(ClosestPair.class);
    Shape shapeClass = params.getShape("shape");

    Path outputPath;
    FileSystem outFs = FileSystem.get(job.getConfiguration());
    if(out==null){
      do {
        outputPath = new Path(inPaths[0].toUri().getPath()+
            ".sample_"+(int)(Math.random()*1000000));
      } while (outFs.exists(outputPath));
    } else {
      outputPath = out;
    }
    // Set map and reduce
    job.setMapperClass(DENDISSamplerMap.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(shapeClass.getClass());

    // Set input and output
    job.setInputFormatClass(SpatialInputFormat3.class);
    SpatialInputFormat3.setInputPaths(job, inPaths);
    job.setOutputFormatClass(TextOutputFormat3.class);
    TextOutputFormat.setOutputPath(job, outputPath);

    // Submit the job
    if (!params.getBoolean("background", false)) {
      job.waitForCompletion(params.getBoolean("verbose", false));
      if (!job.isSuccessful())
        throw new RuntimeException("Job failed!");
    } else {
      job.submit();
    }

    if(output!=null) {
      // Return all records from the output
      final SpatialInputFormat3<Rectangle, Shape> inputFormat =
          new SpatialInputFormat3<Rectangle, Shape>();

      SpatialInputFormat3.setInputPaths(job, outputPath);
      List<InputSplit> splits = inputFormat.getSplits(job);
      for (InputSplit split : splits) {
        FileSplit fsplit = (FileSplit) split;
        org.apache.hadoop.mapreduce.RecordReader<Rectangle, Iterable<Shape>> reader =
            inputFormat.createRecordReader(fsplit, null);
        if (reader instanceof SpatialRecordReader3) {
          ((SpatialRecordReader3)reader).initialize(fsplit, params);
        } else if (reader instanceof RTreeRecordReader3) {
          ((RTreeRecordReader3)reader).initialize(fsplit, params);
        } else if (reader instanceof HDFRecordReader) {
          ((HDFRecordReader)reader).initialize(fsplit, params);
        } else {
          throw new RuntimeException("Unknown record reader");
        }

        while (reader.nextKeyValue()) {
          Iterable<Shape> shapes = reader.getCurrentValue();
          for (Shape shape : shapes) {
            output.collect((T)shape);
          }
        }

        reader.close();
      }
    }

    if(out==null)
      outFs.delete(outputPath, true);

    return job;
  }

  public static Job dendisSampler(Path[] inFiles, Path outPath, OperationsParams params)
      throws IOException, InterruptedException,      ClassNotFoundException {
    return dendisSamplerMapReduce(inFiles, outPath, null, params);
  }

  private static void printUsage() {
    System.out.println("ClosestPair");
    System.out.println("Computes the closest pair of points in the input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to file that contains all shapes");
    System.out.println("shape:<s> - Type of shapes stored in the input file");
    System.out.println("-local - Implement a local machine algorithm (no MapReduce)");
  }

  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    GenericOptionsParser parser = new GenericOptionsParser(args);
    OperationsParams params = new OperationsParams(parser);

    if (!params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }

    Path[] inFiles = params.getInputPaths();
    Path outPath = params.getOutputPath();

    long t1 = System.currentTimeMillis();
    Job job = dendisSampler(inFiles, outPath, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: " + (t2 - t1) + " millis");
    if (job != null) {
      System.out.println("Input points: "+job.getCounters().findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue());
      System.out.println("Map output points: "+job.getCounters().findCounter(Task.Counter.MAP_OUTPUT_RECORDS).getValue());
    }
  }

  public static <T extends Shape> void sample(Path[] inputFiles,
                                                        ResultCollector<T> output, OperationsParams params)
      throws IOException {
    if (params.get("ratio") != null) {
      try {
        DENDISSampler.dendisSamplerMapReduce(inputFiles, null, output, params);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException("Must provide one of three options 'ratio'");
    }
  }

}
