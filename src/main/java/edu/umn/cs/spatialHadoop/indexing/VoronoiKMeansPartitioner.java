/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import de.lmu.ifi.dbs.elki.algorithm.clustering.kmeans.KMeansSort;
import de.lmu.ifi.dbs.elki.algorithm.clustering.kmeans.initialization.KMeansPlusPlusInitialMeans;
import de.lmu.ifi.dbs.elki.data.Cluster;
import de.lmu.ifi.dbs.elki.data.Clustering;
import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.data.model.KMeansModel;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.database.StaticArrayDatabase;
import de.lmu.ifi.dbs.elki.datasource.ArrayAdapterDatabaseConnection;
import de.lmu.ifi.dbs.elki.datasource.DatabaseConnection;
import de.lmu.ifi.dbs.elki.distance.distancefunction.minkowski.SquaredEuclideanDistanceFunction;
import de.lmu.ifi.dbs.elki.logging.LoggingConfiguration;
import de.lmu.ifi.dbs.elki.utilities.random.RandomFactory;
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
import java.util.Vector;

/**
 * @author Ahmed Eldawy
 *
 */
public class VoronoiKMeansPartitioner extends VoronoiPartitioner {

  public double convergenceDis = 0.01;

  @Override
  protected void calculate() {
    // Set the logging level to statistics:
    LoggingConfiguration.setStatistics();

    // Generate a random data set.
    // Note: ELKI has a nice data generator class, use that instead.
    double[][] data = new double[this.dataSet.length][2];
    for(int i = 0; i < data.length; i++) {
      data[i][0]=this.dataSet[i].x;
      data[i][1]=this.dataSet[i].y;
    }

    this.dataSet = null;

    // Adapter to load data from an existing array.
    DatabaseConnection dbc = new ArrayAdapterDatabaseConnection(data);
    // Create a database (which may contain multiple relations!)
    Database db = new StaticArrayDatabase(dbc, null);
    // Load the data into the database (do NOT forget to initialize...)
    db.initialize();

    // K-means should be used with squared Euclidean (least squares):
    SquaredEuclideanDistanceFunction dist = SquaredEuclideanDistanceFunction.STATIC;
    // Default initialization, using global random:
    // To fix the random seed, use: new RandomFactory(seed);
    KMeansPlusPlusInitialMeans init = new KMeansPlusPlusInitialMeans(RandomFactory.DEFAULT);

    // Textbook k-means clustering:
    KMeansSort<NumberVector> km = new KMeansSort<NumberVector>(dist, //
        k /* k - number of partitions */, //
        0 /* maximum number of iterations: no limit */, init);

    // K-means will automatically choose a numerical relation from the data set:
    // But we could make it explicit (if there were more than one numeric
    // relation!): km.run(db, rel);
    Clustering<KMeansModel> c = km.run(db);

    // Output all clusters:
    int i = 0;
    k = c.getAllClusters().size();
    centers = new Point[k];
    pivotInfo = new PivotInfo[k];
    System.out.println("clusters: "+k);
    for(Cluster<KMeansModel> clu : c.getAllClusters()) {
      // K-means will name all clusters "Cluster" in lack of noise support:
      System.out.println("#" + i + ": " + clu.getNameAutomatic());
      System.out.println("Size: " + clu.size());
      System.out.println("Center: " + clu.getModel().getPrototype().toString());
      double[] pivot = clu.getModel().getPrototype();
      System.out.println(pivot[0]);
      System.out.println(pivot[1]);
      centers[i] = new Point(pivot[0], pivot[1]);
      pivotInfo[i] = new PivotInfo(i,centers[i]);
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
      System.out.println();
      ++i;
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
    VoronoiKMeansPartitioner hcp = new VoronoiKMeansPartitioner();
    hcp.createFromPoints(inMBR, points.toArray(new Point[0]), 10);
    
    System.out.println("x,y,partition");
    for (Point p : points) {
      int partition = hcp.overlapPartition(p);
      System.out.println(p.x+","+p.y+","+partition);
    }
  }

}
