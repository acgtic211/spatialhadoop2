/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.PivotInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.Vector;

/**
 * @author Ahmed Eldawy
 *
 */
public class VoronoiRandomPartitioner extends VoronoiPartitioner {

  @Override
  protected void calculate() {
    int setNum = Math.max(1,(int)Math.floor((float)dataSet.length/(float)k));
    double maxSum = -Double.MAX_VALUE;
    Random r = new Random();
    for(int n=0; n < setNum; n++) {
      int rn = Math.abs(r.nextInt(dataSet.length));

      Point[] newCenters = new Point[k];
      Vector<Integer> ids = new Vector<Integer>();

      for (int i = 0; i < this.k; i++) {
        while (ids.contains(rn)) {
          rn = Math.abs(r.nextInt(dataSet.length));
        }

        newCenters[i] = dataSet[rn].clone();

        rn = r.nextInt(dataSet.length);
      }

      double sum = 0;
      for(int i = 0; i < newCenters.length; i++) {
        for(int j = i+1; j < newCenters.length; j++) {
          sum += newCenters[i].distanceTo(newCenters[j]);
        }

      }

      if(sum>maxSum){
        this.centers = newCenters;
      }

    }

    Arrays.sort(centers);
    for(int n = 0; n < centers.length; n++){
      Point center = centers[n];
      pivotInfo[n] = new PivotInfo(n,center);
      System.out.println(pivotInfo[n]);
    }
  }


  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    
    Path inPath = params.getInputPath();
    long length = inPath.getFileSystem(params).getFileStatus(inPath).getLen();
    ShapeIterRecordReader reader = new ShapeIterRecordReader(params,
        new FileSplit(inPath, 0, length, new String[0]));
    Rectangle key = reader.createKey();
    ShapeIterator shapes = reader.createValue();
    final Vector<Point> points = new Vector<Point>();
    while (reader.next(key, shapes)) {
      for (Shape s : shapes) {
        points.add(s.getMBR().getCenterPoint());
      }
    }
    Rectangle inMBR = (Rectangle)OperationsParams.getShape(params, "mbr");
    VoronoiRandomPartitioner hcp = new VoronoiRandomPartitioner();
    hcp.createFromPoints(inMBR, points.toArray(new Point[points.size()]), 10);
    
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = hcp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
  }

}
