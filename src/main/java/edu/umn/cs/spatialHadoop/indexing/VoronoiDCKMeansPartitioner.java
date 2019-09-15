package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Point;

import java.util.ArrayList;
import java.util.HashMap;

public class VoronoiDCKMeansPartitioner extends VoronoiKMeansPartitioner {

  private class PointID implements Comparable<PointID>   {
    Point r;
    int id;
    double dist;

    PointID(Point r, int id, double dist){
      this.r =r ;
      this.dist =dist;
      this.id = id;
    }

    @Override
    public int compareTo(PointID rect2) {
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
  protected void initKCenters() {
    ArrayList<PointID> newCenters = new ArrayList<PointID>();

    double meanDis = calculateMeanDis();
    HashMap<Integer,PointID> remainingPoints = new HashMap<Integer,PointID>();

    PointID C1 = null;
    double maxDensity = Double.NEGATIVE_INFINITY;

    for(int n = 0 ; n < this.dataSet.length; n++) {
      PointID sample = new PointID(this.dataSet[n],n,calculateP(this.dataSet[n],meanDis));
      remainingPoints.put(n, sample);
      if(maxDensity<sample.dist){
        maxDensity = sample.dist;
        C1 = sample;
      }
    }

    ArrayList<PointID> clusterPoints = findCluster(C1,remainingPoints,meanDis);
    removeClusterPoints(remainingPoints, clusterPoints);
    newCenters.add(C1);

    PointID C2 = null;
    double maxWeight = Double.NEGATIVE_INFINITY;
    while (!remainingPoints.isEmpty()){
      for(PointID pi : remainingPoints.values()) {
        double a = calculateA(pi, remainingPoints, meanDis);
        double s = calculateS(pi, remainingPoints);
        double w = pi.dist * s / a;

        if(w>maxWeight){
          maxWeight = w;
          C2 = pi;
        }
      }

      ArrayList<PointID> cluster2Points = findCluster(C2,remainingPoints,meanDis);
      removeClusterPoints(remainingPoints, cluster2Points);
      newCenters.add(C2);
    }

    this.k = newCenters.size();
    centers = new Point[k];
    for(int i = 0; i < k; i++){
      centers[i] = newCenters.get(i).r;
    }

  }

  private void removeClusterPoints(HashMap<Integer,PointID> remainingPoints, ArrayList<PointID> clusterPoints) {
    for(PointID sample : clusterPoints){
      remainingPoints.remove(sample.id);
    }
  }

  private ArrayList<PointID> findCluster(PointID c1, HashMap<Integer,PointID> remainingPoints, double meanDis) {
    ArrayList<PointID> clusters = new ArrayList<PointID>();

    for(PointID sample : remainingPoints.values()){
      double distance = c1.r.distanceTo(sample.r);
      if(distance < meanDis){
        clusters.add(sample);
      }
    }

    return clusters;
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
    for (Point pj : dataSet) {
      double x = pi.distanceTo(pj) - meanDis;
      if (x < 0) {
        p++;
      }
    }
    return p;
  }

  private double calculateA(PointID pi, HashMap<Integer,PointID> remainingPoints, double meanDis){
    double a;

    ArrayList<PointID> cluster = findCluster(pi, remainingPoints, meanDis);
    double sumDistances = 0;
    for(int i = 0; i < cluster.size(); i++){
      for(int j = i; j < cluster.size(); j++){
        double distance = cluster.get(i).r.distanceTo(cluster.get(j).r);
        sumDistances += distance;
      }
    }

    int p = cluster.size();

    a = 2 * sumDistances / (p*(p-1));

    return a;
  }

  private double calculateS(PointID pi, HashMap<Integer, PointID> remainingPoints) {
    double s;
    boolean existsJ = false;
    double minDist = Double.POSITIVE_INFINITY;
    double maxDist = Double.NEGATIVE_INFINITY;
    for(PointID pj : remainingPoints.values()){

      if(pj.id == pi.id)
        continue;

      double distance = pi.r.distanceTo(pj.r);

      if(pj.dist>pi.dist){
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
}
