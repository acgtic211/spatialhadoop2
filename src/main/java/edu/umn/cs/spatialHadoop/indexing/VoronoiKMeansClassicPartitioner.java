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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

/**
 * @author Ahmed Eldawy
 *
 */
public class VoronoiKMeansClassicPartitioner extends VoronoiPartitioner {

  public double convergenceDis = 0.01;

  @Override
  protected void calculate() {
    int round = 0;
    int vote;
    initKCenters();
    convergence = false;

    while (!convergence) {
      int changedCenterNumber = 0;
      dif = 0;
      vote = 0;
      for (int i = 0; i < dataSet.length; i++) {
        double minDistance = Double.MAX_VALUE;
        for (int j = 0; j < this.k; j++) {
          double currentDis = dataSet[i].distanceTo(centers[j]);
          if (currentDis < minDistance) {
            minDistance = currentDis;
            clusters[i] = j;
          }
        }
      }
      for (int i = 0; i < this.k; i++) {
        Point temp = new Point();
        int count = 0;
        for (int j = 0; j < clusters.length; j++) {
          if (clusters[j] == i) {
            temp.x += dataSet[j].x;
            temp.y += dataSet[j].y;
            count++;
          }
        }
        if (count != 0) {
          temp.x /= count;
          temp.y /= count;
          if (isCenterConvergence(centers[i], temp)) {
            vote++;
          }
          centers[i] = temp.clone();
          changedCenterNumber++;
        }

      }
      dif = dif / centers.length;
      if (vote == changedCenterNumber || dif < convergenceDis) {
        convergence = true;
      }
      System.out.println("==================round: " + round++ +" == vote: "+vote+" == changed: "+changedCenterNumber+" == dif: "+ dif);

    }

    ArrayList<Point> newCenters = new ArrayList<Point>();
    for (int i = 0; i < this.k; i++) {
      int count = 0;
      for (int cluster : clusters) {
        if (cluster == i) {
          count++;
        }
      }
      if (count != 0) {
        newCenters.add(centers[i]);
      }
    }

    k = newCenters.size();

    centers = new Point[k];
    pivotInfo = new PivotInfo[k];
    newCenters.toArray(centers);
    Arrays.sort(centers);

    for(int n = 0; n < centers.length; n++){
      Point center = centers[n];
      pivotInfo[n] = new PivotInfo(n,center);
      System.out.println(pivotInfo[n]);
    }
  }

  private boolean isCenterConvergence(Point center, Point pCenter) {
    boolean result = true;
    dif += Math.abs(center.distanceTo(pCenter));
    if (Math.abs(center.x-pCenter.x) > convergenceDis) {
      result = false;
    }
    if (Math.abs(center.y-pCenter.y) > convergenceDis) {
      result = false;
    }
    return result;
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
    VoronoiKMeansClassicPartitioner hcp = new VoronoiKMeansClassicPartitioner();
    hcp.createFromPoints(inMBR, points.toArray(new Point[0]), 10);
    
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = hcp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
  }

}
