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
import java.util.*;

/**
 * @author Ahmed Eldawy
 *
 */
public class VoronoiKMedoidsPartitioner extends VoronoiPartitioner {

  class PointNN implements Comparable<PointNN>   {
    Point r;
    double dist;

    PointNN(Point r, double dist){
      this.r =r ;
      this.dist =dist;
    }

    @Override
    public int compareTo(PointNN rect2) {
      double difference = this.dist - rect2.dist;
      if (difference < 0) {
        return -1;
      }
      if (difference > 0) {
        return 1;
      }
      return 0;

    }

  }

  @Override
  protected void calculate() {

    initKCenters();

    ArrayList<Point>[] partitions = new ArrayList[k];
    for(int i = 0; i < partitions.length; i++){
      partitions[i] = new ArrayList<Point>();
    }

    for (Point point : dataSet) {
      double minDistance = Double.POSITIVE_INFINITY;
      int currentCenter = -1;
      for (int j = 0; j < centers.length; j++) {
        double currentDis = point.distanceTo(centers[j]);
        if (currentDis < minDistance) {
          minDistance = currentDis;
          currentCenter = j;
        }
      }
      partitions[currentCenter].add(point);
    }

    Random rand = new Random(1);
    boolean notConvergence = true;
    int bestCost = Integer.MAX_VALUE;
    int round = 0;
    while(notConvergence) {

      Point[] newCenters = new Point[k];

      for (int i = 0; i < this.k; i++) {
        //STEP 1
        int rn = rand.nextInt(partitions[i].size());
        PointNN sz = new PointNN(partitions[i].get(rn).clone(), 0);

        //STEP 2
        TreeSet<PointNN> distances = new TreeSet<PointNN>();

        for (Point aDataSet1 : partitions[i]) {
          PointNN aux = new PointNN(aDataSet1, sz.r.distanceTo(aDataSet1));
          distances.add(aux);
        }

        PointNN sI = distances.last();

        distances.clear();

        //STEP3
        ArrayList<Double> distancesToSI = new ArrayList<Double>();

        for (Point aDataSet : partitions[i]) {
          double dsI = sI.r.distanceTo(aDataSet);
          distancesToSI.add(dsI);

          PointNN aux = new PointNN(aDataSet, dsI);
          distances.add(aux);

        }

        PointNN sII = distances.last();
        distances.clear();

        //STEP 4

        ArrayList<Double> distancesToSII = new ArrayList<Double>();
        ArrayList<PointNN> X = new ArrayList<PointNN>();

        for (int j = 0; j < partitions[i].size(); j++) {

          Point si = partitions[i].get(j);

          double dsI = distancesToSI.get(j);

          double dsII = sII.r.distanceTo(si);
          distancesToSII.add(dsII);

          double xi = ((dsI * dsI) + (sII.dist * sII.dist) - (dsII * dsII)) / (2 * (sII.dist));

          PointNN aux = new PointNN(partitions[i].get(j), xi);
          X.add(aux);
        }

        //STEP 5
        Collections.sort(X);

        PointNN sMd = X.get(X.size() / 2);

        double m = sMd.dist;

        //STEP 6

        double min = Double.POSITIVE_INFINITY;

        Point r = null;

        for (int j = 0; j < partitions[i].size(); j++) {

          Point si = partitions[i].get(j);

          double dsI = distancesToSI.get(j);
          double dsII = distancesToSII.get(j);

          double minI = Math.abs(dsI - m) + Math.abs(dsII - (sII.dist - m));

          if (minI < min) {
            r = si;
            min = minI;
          }
        }

        newCenters[i] = r;

      }

      ArrayList<Point>[] newPartitions = new ArrayList[k];
      for(int i = 0; i < newPartitions.length; i++){
        newPartitions[i] = new ArrayList<Point>();
      }

      int totalCost = 0;
      for (Point point : dataSet) {
        double minDistance = Double.POSITIVE_INFINITY;
        int currentCenter = -1;
        for (int j = 0; j < centers.length; j++) {
          double currentDis = point.distanceTo(centers[j]);
          if (currentDis < minDistance) {
            minDistance = currentDis;
            currentCenter = j;
          }
        }
        newPartitions[currentCenter].add(point);
        totalCost += minDistance;
      }

      notConvergence = false;

      /*for(int i = 0; i < newPartitions.length; i++){
         if(newPartitions[i].size()!=partitions[i].size()){
          notConvergence = true;
          partitions = newPartitions;
          break;
        }
      }*/

      for(int i = 0; i < newCenters.length; i++){
        if(!newCenters[i].equals(centers[i])){
          notConvergence = true;
          partitions = newPartitions;
          break;
        }
      }

      System.out.println("==================round: " + round++ +" == cost: "+totalCost+" == best: "+bestCost);

      if(totalCost>=bestCost)
      {
        notConvergence = false;
        //centers = newCenters;

        continue;
      }

      bestCost = totalCost;
      centers = newCenters;

    }

    for(int i = 0; i < centers.length; i++){
      pivotInfo[i] = new PivotInfo(i, centers[i]);
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
    VoronoiKMedoidsPartitioner hcp = new VoronoiKMedoidsPartitioner();
    hcp.createFromPoints(inMBR, points.toArray(new Point[0]), 10);
    
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = hcp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
  }

}
