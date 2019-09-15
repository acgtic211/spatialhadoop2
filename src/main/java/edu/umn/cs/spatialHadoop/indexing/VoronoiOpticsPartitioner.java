/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import de.lmu.ifi.dbs.elki.algorithm.clustering.optics.FastOPTICS;
import de.lmu.ifi.dbs.elki.algorithm.clustering.optics.OPTICSList;
import de.lmu.ifi.dbs.elki.algorithm.clustering.optics.OPTICSXi;
import de.lmu.ifi.dbs.elki.data.Cluster;
import de.lmu.ifi.dbs.elki.data.Clustering;
import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.data.model.OPTICSModel;
import de.lmu.ifi.dbs.elki.data.type.TypeUtil;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.database.StaticArrayDatabase;
import de.lmu.ifi.dbs.elki.database.ids.DBIDIter;
import de.lmu.ifi.dbs.elki.database.relation.Relation;
import de.lmu.ifi.dbs.elki.datasource.ArrayAdapterDatabaseConnection;
import de.lmu.ifi.dbs.elki.datasource.DatabaseConnection;
import de.lmu.ifi.dbs.elki.distance.distancefunction.minkowski.EuclideanDistanceFunction;
import de.lmu.ifi.dbs.elki.index.IndexFactory;
import de.lmu.ifi.dbs.elki.index.preprocessed.fastoptics.RandomProjectedNeighborsAndDensities;
import de.lmu.ifi.dbs.elki.index.tree.metrical.covertree.SimplifiedCoverTree;
import de.lmu.ifi.dbs.elki.logging.LoggingConfiguration;
import de.lmu.ifi.dbs.elki.utilities.ELKIBuilder;
import de.lmu.ifi.dbs.elki.utilities.datastructures.iterator.It;
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
import java.util.List;
import java.util.Random;
import java.util.Vector;

/**
 * @author Ahmed Eldawy
 *
 */
public class VoronoiOpticsPartitioner extends VoronoiPartitioner {

  @Override
  protected void calculate() {
    // Set the logging level to statistics:
    LoggingConfiguration.setStatistics();

    double[][] data;
    if(dataSet.length>100000) {
      Random random = new Random(1);
      ArrayList<double[]> finalDataset = new ArrayList<double[]>();
      double rho = 100000.0 / dataSet.length;
      for(Point p : this.dataSet){
        if(random.nextDouble()<rho){
          double[] point = new double[2];
          point[0] = p.x;
          point[1] = p.y;
          finalDataset.add(point);
        }
      }
      System.out.println(finalDataset.size());
      data = new double[finalDataset.size()][];
      data = finalDataset.toArray(data);
    } else {
      // Generate a random data set.
      // Note: ELKI has a nice data generator class, use that instead.
      data = new double[this.dataSet.length][2];
      for (int i = 0; i < data.length; i++) {
        data[i][0] = this.dataSet[i].x;
        data[i][1] = this.dataSet[i].y;
      }
    }

    this.dataSet = null;

    // Adapter to load data from an existing array.
    DatabaseConnection dbc = new ArrayAdapterDatabaseConnection(data);
    // Create a database (which may contain multiple relations!)

    SimplifiedCoverTree.Factory<NumberVector> factory = new ELKIBuilder<SimplifiedCoverTree.Factory<NumberVector>>(SimplifiedCoverTree.Factory.class) //
        .with(SimplifiedCoverTree.Factory.Parameterizer.DISTANCE_FUNCTION_ID, EuclideanDistanceFunction.class).build();

    List<IndexFactory<NumberVector>> list = new ArrayList<IndexFactory<NumberVector>>();
    list.add(factory);

    Database db = new StaticArrayDatabase(dbc, list);
    // Load the data into the database (do NOT forget to initialize...)
    db.initialize();
    // Relation containing the number vectors:
    Relation<NumberVector> rel = db.getRelation(TypeUtil.NUMBER_VECTOR_FIELD);

    Clustering<OPTICSModel> c = new ELKIBuilder<OPTICSXi>(OPTICSXi.class) //
        .with(OPTICSList.Parameterizer.MINPTS_ID, 100) //
        .with(OPTICSXi.Parameterizer.XI_ID, 0.025) //
        .with(OPTICSXi.Parameterizer.XIALG_ID, FastOPTICS.class) //
        .with(RandomProjectedNeighborsAndDensities.Parameterizer.RANDOM_ID, 0) //
        .build().run(db);

    // Output all clusters:
    int i = 0;
    List<Cluster<OPTICSModel>> clusters = c.getToplevelClusters();
    List<Cluster<OPTICSModel>> finalClusters = new ArrayList<Cluster<OPTICSModel>>();
    while(!clusters.isEmpty()){// K-means will name all clusters "Cluster" in lack of noise support:
      Cluster<OPTICSModel> clu = clusters.get(0);
      //System.out.println("#" + i + ": " + clu.getNameAutomatic());
      //System.out.println("Size: " + clu.size());
      int numChildren = c.getClusterHierarchy().numChildren(clu);
      clusters.remove(0);
      //System.out.println();
      if(numChildren>0){
        for(It<Cluster<OPTICSModel>> children = c.getClusterHierarchy().iterChildren(clu); children.valid(); children.advance()) {
          Cluster<OPTICSModel> clu2 = children.get();
          clusters.add(clu2);
          if((clusters.size()+finalClusters.size())>=k)
            break;
        }
      } else {
        finalClusters.add(clu);
      }
      //System.out.println("Center: " + clu.getModel().getPrototype().toString());
      //double[] pivot = clu.getModel();.getPrototype();
      //centers[i] = new Point(pivot[0], pivot[1]);
      //pivotInfo[i] = new PivotInfo(i,centers[i]);
      // Iterate over objects:
      /*System.out.print("Objects: ");
      for(DBIDIter it = clu.getIDs().iter(); it.valid(); it.advance()) {
        // To get the vector use:
        // NumberVector v = rel.get(it);

        // Offset within our DBID range: "line number"
        final int offset = ids.getOffset(it);
        System.out.print(" " + offset);
        // Do NOT rely on using "internalGetIndex()" directly!
      }*/
      if((clusters.size()+finalClusters.size())>=k)
        break;
      ++i;
    }
    finalClusters.addAll(clusters);
    k = finalClusters.size();
    centers = new Point[k];
    pivotInfo = new PivotInfo[k];
    i = 0;
    for (Cluster<OPTICSModel> clu: finalClusters) {
      System.out.println("#" + i + ": " + clu.getNameAutomatic());
      System.out.println("Size: " + clu.size());
      System.out.println();
      double[] pivot = {0.0,0.0};
      for(DBIDIter it = clu.getIDs().iter(); it.valid(); it.advance()) {
        // To get the vector use:
        NumberVector v = rel.get(it);
        double[] point = v.toArray();
        pivot[0] = (pivot[0]+point[0])/2;
        pivot[1] = (pivot[1]+point[1])/2;
        // Do NOT rely on using "internalGetIndex()" directly!
      }
      centers[i] = new Point(pivot[0], pivot[1]);
      pivotInfo[i] = new PivotInfo(i,centers[i]);
      i++;
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
    VoronoiOpticsPartitioner hcp = new VoronoiOpticsPartitioner();
    hcp.createFromPoints(inMBR, points.toArray(new Point[0]), 10);
    
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = hcp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
  }

}
