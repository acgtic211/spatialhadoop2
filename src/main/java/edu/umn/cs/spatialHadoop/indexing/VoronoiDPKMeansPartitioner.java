package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Point;

import java.util.*;

public class VoronoiDPKMeansPartitioner extends VoronoiKMeansPartitioner {

  private class PointID implements Comparable<PointID>   {
    Point r;
    int id;
    int p;
    double dist;

    PointID(Point r, int id, int p, double dist){
      this.r =r ;
      this.p = p;
      this.dist =dist;
      this.id = id;
    }

    @Override
    public int compareTo(PointID rect2) {
      double difference = this.p - rect2.p;
      if (difference < 0) {
        return -1;
      }
      if (difference > 0) {
        return 1;
      }
      return (int)(this.dist - rect2.dist);

    }

  }

  @Override
  protected void calculate() {

    int oldK = k;
    k = 10 * k;

    super.calculate();

    k = oldK;

    double meanDis = calculateMeanDis();

    ArrayList<PointID> newCenters = new ArrayList<PointID>();
    for(int n = 0 ; n < this.centers.length; n++) {
      Point p = this.centers[n];
      int pi = calculateP(p,meanDis);
      PointID sample = new PointID(this.centers[n],n,pi,0);
      newCenters.add(sample);
    }

    PriorityQueue<PointID> sortedCenters =
        new PriorityQueue<PointID>();
    for(PointID p : newCenters){
      p.dist = calculateS(p,newCenters, meanDis);
      sortedCenters.add(p);
    }

    this.centers = new Point[k];
    for(int n = 0; n < centers.length; n++){
      centers[n] = sortedCenters.poll().r;
    }

  }


  private double calculateMeanDis(){
    double d = 0;
    for(int i = 0; i < dataSet.length; i++){
      for(int j = i+1; j < dataSet.length; j++){
        d += dataSet[i].distanceTo(dataSet[j]);
      }
    }

    d = 2*d / (dataSet.length*(dataSet.length-1));

    return d;
  }

  private int calculateP(Point pi, double meanDis){
    int p = 0;
    for (Point pj : centers) {
      double x = pi.distanceTo(pj) - meanDis;
      if (x < 0) {
        p++;
      }
    }
    return p;
  }

  private double calculateS(PointID pi, List<PointID> remainingPoints, double meanDis) {
    double s;
    boolean existsJ = false;
    double minDist = Double.POSITIVE_INFINITY;
    double maxDist = Double.NEGATIVE_INFINITY;
    for(PointID pj : remainingPoints){

      if(pj.id == pi.id)
        continue;

      double distance = pi.r.distanceTo(pj.r);

      if(pj.p>pi.p && !densityConnection(pi,pj,distance,meanDis).equals("non")){
        existsJ = true;
        if(distance<minDist){
          minDist = distance;
        }
      } else if(distance>maxDist){
        maxDist = distance;
      }

    }

    s = existsJ?minDist:maxDist;

    return s;
  }

  private String densityConnection(PointID pi, PointID pj, double distance, double meanDis){

    if(distance<meanDis){
      return "cn";
    }

    if(meanDis <= distance && distance < 2*meanDis){
      int pMean = calculateP(new Point((pi.r.x+pj.r.x)/2,(pi.r.y+pj.r.y)/2),meanDis);
      int pMin = Math.min(pi.p,pj.p);
      if(pMean>=pMin){
        return  "icn";
      }
    }

    return "non";
  }
}
