package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.*;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

public abstract class VoronoiPartitioner extends Partitioner implements TextSerializable {

  /**The MBR of the original input file*/
  protected final Rectangle mbr = new Rectangle();

  protected int[] clusters;
  protected Point[] dataSet;
  protected int k = 1;
  protected Point[] centers;
  protected boolean readOnly = false;

  protected PivotInfo[] pivotInfo;

  protected boolean convergence;
  protected double dif = 0;
  protected int replicates = 0;

  public PivotInfo[] getPivotInfo() {
    return pivotInfo;
  }

  protected void initKCenters() {

    Random r = new Random(1);
    int rn = r.nextInt(dataSet.length);
    for (int i = 0; i < this.k; i++)
    {
      centers[i] = dataSet[rn].clone();

      rn = r.nextInt(dataSet.length);
    }

  }

  @Override
  public void createFromPoints(Rectangle mbr, Point[] points, int capacity) {

    this.mbr.set(mbr);

    clusters = new int[points.length];
    this.dataSet = points;
    this.k = (int) Math.ceil((double)points.length / (capacity));
    this.centers = new Point[k];
    this.pivotInfo = new PivotInfo[k];

    System.out.println("K : "+k);
    calculate();

  }

  protected abstract void calculate();

  @Override
  public void write(DataOutput out) throws IOException {
    mbr.write(out);
    out.writeInt(centers.length);
    for (Point center : centers)
      center.write(out);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    mbr.readFields(in);
    centers = new Point[in.readInt()];
    pivotInfo = new PivotInfo[centers.length];
    for (int i = 0; i < centers.length; i++) {
      centers[i] = new Point();
      centers[i].readFields(in);
      pivotInfo[i] = new PivotInfo(i,centers[i].x,centers[i].y);
    }

  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    if (shape == null)
      return;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return;

    Point center = shapeMBR.getCenterPoint();
    double radius = shapeMBR.getHeight() / 2;

    for (int i = 0; i < centers.length; i++) {
      Point pivotCenter = centers[i];
      PivotInfo pivot = pivotInfo[i];
      double distance = pivotCenter.distanceTo(center) - (pivot.maxDis + radius);
      if(distance<0){
        matcher.collect(i);
      }

    }

    /*int partition = Arrays.binarySearch(centers,center);

    if(partition<0){
      partition = -partition - 1;
    }

    int pr = partition;
    int pl = pr - 1;

    boolean flagPL = pl < 0;
    boolean flagPR = pr > centers.length - 1;

    double gdmax = radius;

    while (!flagPL || !flagPR) {

      double dxL = flagPL?Double.MAX_VALUE:Math.abs(centers[pl].x - center.x) - (pivotInfo[pl].maxDis + radius);
      double dxR = flagPR?Double.MAX_VALUE:Math.abs(centers[pr].x - center.x) - (pivotInfo[pr].maxDis + radius);

      if(dxL<dxR){

        if(dxL>0){
          //flagPL = true;
          pl--;
          flagPL = pl < 0;
          continue;
        }

        double distance = centers[pl].distanceTo(center) - (pivotInfo[pl].maxDis + radius);

        if(distance<0){

          matcher.collect(pl);

        }

        pl--;
        flagPL = pl < 0;

      }else{

        if(dxR>0){
          //flagPR = true;
          pr++;
          flagPR = pr > centers.length - 1;
          continue;
        }

        double distance = centers[pr].distanceTo(center) - (pivotInfo[pr].maxDis + radius);

        if(distance<0){

          matcher.collect(pr);

        }

        pr++;
        flagPR = pr > centers.length - 1;

      }
    }*/
  }

  @Override
  public int overlapPartition(Shape shape) {
    if (shape == null)
      return -1;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return -1;

    Point center = shapeMBR.getCenterPoint();


    double minDistance = Double.MAX_VALUE;
    Integer partition = -1;
    for(int n = 0; n < centers.length; n++){
      Point pivot = centers[n];
      double distance = pivot.distanceTo(center);
      if(distance<minDistance){
        partition = n;
        minDistance = distance;
      }
    }

    /*
    int partition = Arrays.binarySearch(centers,center);

    if(partition<0){
      partition = -partition - 1;
    }

    int pr = partition;
    int pl = pr - 1;

    boolean flagPL = pl < 0;
    boolean flagPR = pr > centers.length - 1;

    double gdmax = Double.MAX_VALUE;

    while (!flagPL || !flagPR) {


      if(!flagPL){

        double dxL = Math.abs(centers[pl].x - center.x);

        if(dxL>gdmax){

          flagPL = true;

        } else {
          double distance = centers[pl].distanceTo(center);
          if(distance<gdmax){
            gdmax = distance;
            partition = pl;
          }

          pl--;
          flagPL = pl < 0;
        }

      }

      if(!flagPR){

        double dxR = Math.abs(centers[pr].x - center.x);

        if(dxR>gdmax){

          flagPR = true;

        } else {

          double distance = centers[pr].distanceTo(center);
          if(distance<gdmax){
            gdmax = distance;
            partition = pr;
          }

          pr++;
          flagPR = pr > centers.length - 1;

        }

      }
    }*/

    if(!this.readOnly) {
      if (pivotInfo[partition].maxDis < minDistance) {
        pivotInfo[partition].maxDis = minDistance;
      }

      if (pivotInfo[partition].minDis > minDistance) {
        pivotInfo[partition].minDis = minDistance;
      }
    }

    return partition;
  }

  @Override
  public CellInfo getPartition(int partitionID) {
    PivotInfo pivot = pivotInfo[partitionID];
    Circle circle = new Circle(pivot.x,pivot.y,pivot.maxDis);
    CellInfo cellInfo = new CellInfo(partitionID, circle.getMBR());
    return cellInfo;
  }

  @Override
  public CellInfo getPartitionAt(int index) {
    return getPartition(index);
  }

  @Override
  public int getPartitionCount() {
    return centers.length;
  }

  @Override
  public Text toText(Text text) {
    final byte[] Comma = ",".getBytes();
    TextSerializerHelper.serializeInt(k, text, ',');
    for(PivotInfo point : pivotInfo){
      point.toText(text);
      text.append(Comma, 0, Comma.length);
    }
    return text;
  }

  @Override
  public void fromText(Text text) {
    this.k = TextSerializerHelper.consumeInt(text, ',');
    this.centers = new Point[k];
    this.pivotInfo = new PivotInfo[k];
    for(int i = 0; i < k; i++){
      pivotInfo[i]=new PivotInfo();
      pivotInfo[i].fromText(text);
      centers[i]=new Point(pivotInfo[i]);
      if(text.getLength()>0)
        text.set(text.getBytes(), 1, text.getLength() - 1);
    }
  }

  @Override
  public String toString() {
    Text aux = new Text();
    this.toText(aux);
    return aux.toString();
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

}
