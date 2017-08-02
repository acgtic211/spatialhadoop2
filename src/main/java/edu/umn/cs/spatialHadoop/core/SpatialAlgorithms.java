/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.mapred.PairWritable;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.Progressable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.PriorityQueue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

/**
 * Performs simple algorithms for spatial data.
 *
 * @author Ahmed Eldawy
 *
 */
class RectangleNN implements Comparable<RectangleNN>   {
	Rectangle r;
	float dist;
	public RectangleNN(Rectangle r, float dist){
		this.r =r ;
		this.dist =dist;
	}

	public int compareTo(RectangleNN rect2) {
		float difference = this.dist - rect2.dist;
		if (difference < 0) {
			return -1;
		}
		if (difference > 0) {
			return 1;
		}
		return 0;

	}

}
class TOPK {
	public TreeSet<RectangleNN> heap;
	public int k;

	public TOPK(int k) {
		heap = new TreeSet<RectangleNN>();
		this.k = k;
	}

	public void add(Rectangle r,float dist) {
		heap.add(new RectangleNN(r, dist));
		if (this.heap.size() > k) {
			// Remove largest element in set (to keep it of size k)
			this.heap.last();
		}

	}
}



public class SpatialAlgorithms {
  public static final Log LOG = LogFactory.getLog(SpatialAlgorithms.class);
  public enum KCPCounters { NUM_OPERATIONS };

  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweepFilterOnly(
	      final List<S1> R, final List<S2> S, final ResultCollector2<S1, S2> output,
	      Reporter reporter)
	      throws IOException {

	  	LOG.debug("Start spatial join plan sweep algorithm !!!");

	    final RectangleID[] Rmbrs = new RectangleID[R.size()];
	    for (int i = 0; i < R.size(); i++) {
	      Rmbrs[i] = new RectangleID(i, R.get(i).getMBR());
	    }
	    final RectangleID[] Smbrs = new RectangleID[S.size()];
	    for (int i = 0; i < S.size(); i++) {
	      Smbrs[i] = new RectangleID(i, S.get(i).getMBR());
	    }

	    final IntWritable count = new IntWritable();
	    int filterCount = SpatialJoin_rectangles(Rmbrs, Smbrs, new OutputCollector<RectangleID, RectangleID>() {
	        @Override
	        public void collect(RectangleID r1, RectangleID r2)
	            throws IOException {
	          //if (R.get(r1.id).isIntersected(S.get(r2.id))) {
	            if (output != null)
	              output.collect(R.get(r1.id), S.get(r2.id));
	            count.set(count.get() + 1);
	          //}
	        }
	    }, reporter);

	      LOG.debug("Filtered result size "+filterCount+", refined result size "+count.get());

	      return count.get();
	}


  /**
   * @param R
   * @param S
   * @param output
   * @return
   * @throws IOException
   */
  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweep(
      List<S1> R, List<S2> S, ResultCollector2<S1, S2> output, Reporter reporter)
      throws IOException {
    int count = 0;

    Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
    	if (o1.getMBR().x1 == o2.getMBR().x1)
    		return 0;
        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
      }
    };

    long t1 = System.currentTimeMillis();
    Collections.sort(R, comparator);
    Collections.sort(S, comparator);

		int i = 0, j = 0;

    try {
      while (i < R.size() && j < S.size()) {
        S1 r;
        S2 s;
        if (comparator.compare(R.get(i), S.get(j)) < 0) {
          r = R.get(i);
          int jj = j;

          while ((jj < S.size())
              && ((s = S.get(jj)).getMBR().x1 <= r.getMBR().x2)) {
            // Check if r and s are overlapping but not the same object
            // for self join
            if (r.isIntersected(s) && !r.equals(s)) {
              if (output != null)
                output.collect(r, s);
              count++;
            }
            jj++;
            if (reporter !=  null)
              reporter.progress();
          }
          i++;
        } else {
          s = S.get(j);
          int ii = i;

          while ((ii < R.size())
              && ((r = R.get(ii)).getMBR().x1 <= s.getMBR().x2)) {
            if (r.isIntersected(s) && !r.equals(s)) {
              if (output != null)
                output.collect(r, s);
              count++;
            }
            ii++;
            if (reporter !=  null)
              reporter.progress();
          }
          j++;
        }
        if (reporter !=  null)
          reporter.progress();
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    return count;
	}


  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweepFilterOnly(
	      final S1[] R, final S2[] S, ResultCollector2<S1, S2> output, Reporter reporter) {
	    int count = 0;

	    final Comparator<Shape> comparator = new Comparator<Shape>() {
	      @Override
	      public int compare(Shape o1, Shape o2) {
	    	if (o1.getMBR().x1 == o2.getMBR().x1)
	    		return 0;
	        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
	      }
	    };

	    long t1 = System.currentTimeMillis();
	    Arrays.sort(R, comparator);
	    Arrays.sort(S, comparator);

	    int i = 0, j = 0;

	    try {
	      while (i < R.length && j < S.length) {
	        S1 r;
	        S2 s;
	        if (comparator.compare(R[i], S[j]) < 0) {
	          r = R[i];
	          int jj = j;

	          while ((jj < S.length)
	              && ((s = S[jj]).getMBR().x1 <= r.getMBR().x2)) {
	            if (r.getMBR().isIntersected(s.getMBR())) {
	              if (output != null)
	                output.collect(r, s);
	              count++;
	            }
	            jj++;

	            if (reporter != null)
	              reporter.progress();
	          }
	          i++;
	        } else {
	          s = S[j];
	          int ii = i;

	          while ((ii < R.length)
	              && ((r = R[ii]).getMBR().x1 <= s.getMBR().x2)) {
	            if (r.getMBR().isIntersected(s.getMBR())) {
	              if (output != null)
	                output.collect(r, s);
	              count++;
	            }
	            ii++;
	          }
	          j++;
	          if (reporter != null)
	            reporter.progress();
	        }
	        if (reporter != null)
	          reporter.progress();
	      }
	    } catch (RuntimeException e) {
	      e.printStackTrace();
	    }
	    long t2 = System.currentTimeMillis();
	    return count;
	  }


  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweep(
      final S1[] R, final S2[] S, ResultCollector2<S1, S2> output, Reporter reporter) {
    int count = 0;

    final Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
    	if (o1.getMBR().x1 == o2.getMBR().x1)
    		return 0;
        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
      }
    };

    long t1 = System.currentTimeMillis();
    Arrays.sort(R, comparator);
    Arrays.sort(S, comparator);

    int i = 0, j = 0;

    try {
      while (i < R.length && j < S.length) {
        S1 r;
        S2 s;
        if (comparator.compare(R[i], S[j]) < 0) {
          r = R[i];
          int jj = j;

          while ((jj < S.length)
              && ((s = S[jj]).getMBR().x1 <= r.getMBR().x2)) {
            if (r.isIntersected(s)) {
              if (output != null)
                output.collect(r, s);
              count++;
            }
            jj++;
            if (reporter != null)
              reporter.progress();
          }
          i++;
        } else {
          s = S[j];
          int ii = i;

          while ((ii < R.length)
              && ((r = R[ii]).getMBR().x1 <= s.getMBR().x2)) {
            if (r.isIntersected(s)) {
              if (output != null)
                output.collect(r, s);
              count++;
            }
            ii++;
            if (reporter != null)
              reporter.progress();
          }
          j++;
        }
        if (reporter != null)
          reporter.progress();
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    return count;
  }

  /**
   * Self join of rectangles. This method runs faster than the general version
   * because it just performs the filter step based on the rectangles.
   * @param output
   * @return
   * @throws IOException
   */
  public static <S1 extends Rectangle, S2 extends Rectangle> int SpatialJoin_rectangles(final S1[] R, final S2[] S,
      OutputCollector<S1, S2> output, Reporter reporter) throws IOException {
    int count = 0;

    final Comparator<Rectangle> comparator = new Comparator<Rectangle>() {
      @Override
      public int compare(Rectangle o1, Rectangle o2) {
    	if (o1.x1 == o2.x1)
    		  return 0;
        return o1.x1 < o2.x1 ? -1 : 1;
      }
    };

    long t1 = System.currentTimeMillis();
    LOG.debug("Spatial Join of "+ R.length+" X " + S.length + "shapes");
    Arrays.sort(R, comparator);
    Arrays.sort(S, comparator);

    int i = 0, j = 0;

    try {
    	 while (i < R.length && j < S.length) {
    	        S1 r;
    	        S2 s;
    	        if (comparator.compare(R[i], S[j]) < 0) {
    	          r = R[i];
    	          int jj = j;

    	          while ((jj < S.length)
    	              && ((s = S[jj]).getMBR().x1 <= r.getMBR().x2)) {
    	            if (r.isIntersected(s)) {
    	              if (output != null)
    	                output.collect(r, s);
    	              count++;
    	            }
    	            jj++;
    	          }
    	          i++;
    	          if (reporter != null)
    	            reporter.progress();
    	        } else {
    	          s = S[j];
    	          int ii = i;

    	          while ((ii < R.length)
    	              && ((r = R[ii]).getMBR().x1 <= s.getMBR().x2)) {
    	            if (r.isIntersected(s)) {
    	              if (output != null)
    	                output.collect(r, s);
    	              count++;
    	            }
    	            ii++;
    	            if (reporter != null)
    	              reporter.progress();
    	          }
    	          j++;
    	        }
    	        if (reporter != null)
    	          reporter.progress();
    	      }

    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();

    return count;
  }


  /**
   * Self join of rectangles. This method runs faster than the general version
   * because it just performs the filter step based on the rectangles.
   * @param rs
   * @param output
   * @return
   * @throws IOException
   */
  public static <S extends Rectangle> int SelfJoin_rectangles(final S[] rs,
      OutputCollector<S, S> output, Progressable reporter) throws IOException {
    int count = 0;

    final Comparator<Rectangle> comparator = new Comparator<Rectangle>() {
      @Override
      public int compare(Rectangle o1, Rectangle o2) {
    	if (o1.x1 == o2.x1)
    		  return 0;
        return o1.x1 < o2.x1 ? -1 : 1;
      }
    };

    long t1 = System.currentTimeMillis();
    Arrays.sort(rs, comparator);

    int i = 0, j = 0;

    try {
      while (i < rs.length && j < rs.length) {
        S r;
        S s;
        if (rs[i].x1 < rs[j].x1) {
          r = rs[i];
          int jj = j;

          while ((jj < rs.length)
              && ((s = rs[jj]).x1 <= r.x2)) {
            if (r != s && r.isIntersected(s)) {
              if (output != null) {
                output.collect(r, s);
              }
              count++;
            }
            jj++;
            if (reporter != null)
              reporter.progress();
          }
          i++;
        } else {
          s = rs[j];
          int ii = i;

          while ((ii < rs.length)
              && ((r = rs[ii]).x1 <= s.x2)) {
            if (r != s && r.isIntersected(s)) {
              if (output != null) {
                output.collect(r, s);
              }
              count++;
            }
            ii++;
            if (reporter != null)
              reporter.progress();
          }
          j++;
        }
        if (reporter != null)
          reporter.progress();
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();

    return count;
  }

  /**
   * MBR of a shape along with its ID. Used to performs the filter step while
   * keeping track of the ID of each object to be able to do the refine step.
   * @author Ahmed Eldawy
   *
   */
  public static class RectangleID extends Rectangle {
    public int id;

    public RectangleID(int id, Rectangle rect) {
      super(rect);
      this.id = id;
    }

    public RectangleID(int id, double x1, double y1, double x2, double y2) {
      super(x1, y1, x2, y2);
      this.id = id;
    }
  }

  /**
   * The general version of self join algorithm which works with arbitrary
   * shapes. First, it performs a filter step where it finds shapes with
   * overlapping MBRs. Second, an optional refine step can be executed to
   * return only shapes which actually overlap.
   * @param R - input set of shapes
   * @param refine - Whether or not to run a refine step
   * @param output - output collector where the results are reported
   * @return - number of pairs returned by the planesweep algorithm
   * @throws IOException
   */
  public static <S extends Shape> int SelfJoin_planeSweep(final S[] R,
      boolean refine, final OutputCollector<S, S> output, Progressable reporter) throws IOException {
    // Use a two-phase filter and refine approach
    // 1- Use MBRs as a first filter
    // 2- Use ConvexHull as a second filter
    // 3- Use the exact shape for refinement
    final RectangleID[] mbrs = new RectangleID[R.length];
    for (int i = 0; i < R.length; i++) {
      mbrs[i] = new RectangleID(i, R[i].getMBR());
    }

    if (refine) {
      final IntWritable count = new IntWritable();
      int filterCount = SelfJoin_rectangles(mbrs, new OutputCollector<RectangleID, RectangleID>() {
        @Override
        public void collect(RectangleID r1, RectangleID r2)
            throws IOException {
          if (R[r1.id].isIntersected(R[r2.id])) {
            if (output != null)
              output.collect(R[r1.id], R[r2.id]);
            count.set(count.get() + 1);
          }
        }
      }, reporter);

      LOG.debug("Filtered result size "+filterCount+", refined result size "+count.get());

      return count.get();
    } else {
      return SelfJoin_rectangles(mbrs, new OutputCollector<RectangleID, RectangleID>() {
        @Override
        public void collect(RectangleID r1, RectangleID r2)
            throws IOException {
          if (output != null)
            output.collect(R[r1.id], R[r2.id]);
        }
      }, reporter);
    }
  }

  /**
   * Remove duplicate points from an array of points. Two points are considered
   * duplicate if both the horizontal and vertical distances are within a given
   * threshold distance.
   * @param allPoints
   * @param threshold
   * @return
   */
  public static Point[] deduplicatePoints(final Point[] allPoints, final float threshold) {
    final BitArray duplicates = new BitArray(allPoints.length);
    int totalNumDuplicates = 0;
    LOG.debug("Deduplicating a list of "+allPoints.length+" points");
    // Remove duplicates to ensure correctness
    Arrays.sort(allPoints, new Comparator<Point>() {
      @Override
      public int compare(Point p1, Point p2) {
        int dx = Double.compare(p1.x, p2.x);
        if (dx != 0)
          return dx;
        return Double.compare(p1.y,  p2.y);
      }
    });

    try {
      List<Integer> numsOfDuplicates = Parallel.forEach(allPoints.length - 1, new Parallel.RunnableRange<Integer>() {
        @Override
        public Integer run(int i1, int i2) {
          int numOfDuplicates = 0;
          for (int i = i1; i < i2; i++) {
            int j = i + 1;
            boolean duplicate = false;
            while (!duplicate && j < allPoints.length && allPoints[i].x + threshold > allPoints[j].x) {
              double dy = Math.abs(allPoints[j].y - allPoints[i].y);
              if (dy < threshold)
                duplicate = true;
              else
                j++;
            }
            if (duplicate) {
              duplicates.set(i, true);
              numOfDuplicates++;
            }
          }
          return numOfDuplicates;
        }
      });
      for (int numOfDuplicates : numsOfDuplicates)
        totalNumDuplicates += numOfDuplicates;
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    if (totalNumDuplicates > 0) {
      LOG.debug("Shrinking the array");
      // Shrinking the array
      final Point[] newAllPoints = new Point[allPoints.length - totalNumDuplicates];
      int newI = 0, oldI1 = 0;
      while (oldI1 < allPoints.length) {
        // Advance to the first non-duplicate point (start of range to be copied)
        while (oldI1 < allPoints.length && duplicates.get(oldI1))
          oldI1++;
        if (oldI1 < allPoints.length) {
          int oldI2 = oldI1 + 1;
          // Advance to the first duplicate point (end of range to be copied)
          while (oldI2 < allPoints.length && !duplicates.get(oldI2))
            oldI2++;
          // Copy the range [oldI1, oldI2[ to the new array
          System.arraycopy(allPoints, oldI1, newAllPoints, newI, oldI2 - oldI1);
          newI += (oldI2 - oldI1);
          oldI1 = oldI2;
        }
      }
      return newAllPoints;
    }
    return allPoints;
  }

  /**
   * Flatten geometries by extracting all internal geometries inside each
   * geometry.
   * @param geoms
   * @return
   */
  public static Geometry[] flattenGeometries(final Geometry[] geoms) {
    int flattenedNumberOfGeometries = 0;
    for (Geometry geom : geoms)
      flattenedNumberOfGeometries += geom.getNumGeometries();
    if (flattenedNumberOfGeometries == geoms.length)
      return geoms;
    Geometry[] flattenedGeometries = new Geometry[flattenedNumberOfGeometries];
    int i = 0;
    for (Geometry geom : geoms)
      for (int n = 0; n < geom.getNumGeometries(); n++)
        flattenedGeometries[i++] = geom.getGeometryN(n);
    return flattenedGeometries;
  }

  /**
   * Group polygons by overlap
   * @param polygons
   * @param prog
   * @return
   * @throws IOException
   */
  public static Geometry[][] groupPolygons(final Geometry[] polygons,
      final Progressable prog) throws IOException {
    // Group shapes into overlapping groups
    long t1 = System.currentTimeMillis();
    RectangleID[] mbrs = new RectangleID[polygons.length];
    for (int i = 0; i < polygons.length; i++) {
      Coordinate[] coords = polygons[i].getEnvelope().getCoordinates();
      double x1 = Math.min(coords[0].x, coords[2].x);
      double x2 = Math.max(coords[0].x, coords[2].x);
      double y1 = Math.min(coords[0].y, coords[2].y);
      double y2 = Math.max(coords[0].y, coords[2].y);

      mbrs[i] = new RectangleID(i, x1, y1, x2, y2);
    }

    // Parent link of the Set Union Find data structure
    final int[] parent = new int[mbrs.length];
    Arrays.fill(parent, -1);

    // Group records in clusters by overlapping
    SelfJoin_rectangles(mbrs, new OutputCollector<RectangleID, RectangleID>(){
      @Override
      public void collect(RectangleID r, RectangleID s)
          throws IOException {
        int rid = r.id;
        while (parent[rid] != -1) {
          int pid = parent[rid];
          if (parent[pid] != -1)
            parent[rid] = parent[pid];
          rid = pid;
        }
        int sid = s.id;
        while (parent[sid] != -1) {
          int pid = parent[sid];
          if (parent[pid] != -1)
            parent[sid] = parent[pid];
          sid = pid;
        }
        if (rid != sid)
          parent[rid] = sid;
      }}, prog);
    mbrs = null;
    // Put all records in one cluster as a list
    Map<Integer, List<Geometry>> groups = new HashMap<Integer, List<Geometry>>();
    for (int i = 0; i < parent.length; i++) {
      int root = parent[i];
      if (root == -1)
        root = i;
      while (parent[root] != -1) {
        root = parent[root];
      }
      List<Geometry> group = groups.get(root);
      if (group == null) {
        group = new Vector<Geometry>();
        groups.put(root, group);
      }
      group.add(polygons[i]);
    }
    long t2 = System.currentTimeMillis();

    Geometry[][] groupedPolygons = new Geometry[groups.size()][];
    int counter = 0;
    for (List<Geometry> group : groups.values()) {
      groupedPolygons[counter++] = group.toArray(new Geometry[group.size()]);
    }
    LOG.debug("Grouped "+parent.length+" shapes into "+groups.size()+" clusters in "+(t2-t1)/1000.0+" seconds");
    return groupedPolygons;
  }


  /**
   * Directly unions the given list of polygons using a safe method that tries
   * to avoid geometry exceptions. First, it tries the buffer(0) method. It it
   * fails, it falls back to the tradition union method.
   * @param polys
   * @param progress
   * @return
   * @throws IOException
   */
  public static Geometry safeUnion(List<Geometry> polys,
      Progressable progress) throws IOException {
    if (polys.size() == 1)
      return polys.get(0);
    Stack<Integer> rangeStarts = new Stack<Integer>();
    Stack<Integer> rangeEnds = new Stack<Integer>();
    rangeStarts.push(0);
    rangeEnds.push(polys.size());
    List<Geometry> results = new ArrayList<Geometry>();

    final GeometryFactory geomFactory = new GeometryFactory();

    // Minimum range size that is broken into two subranges
    final int MinimumThreshold = 10;
    // Progress numerator and denominator
    int progressNum = 0, progressDen = polys.size();

    while (!rangeStarts.isEmpty()) {
      int rangeStart = rangeStarts.pop();
      int rangeEnd = rangeEnds.pop();

      try {
        // Union using the buffer operation
        GeometryCollection rangeInOne = (GeometryCollection) geomFactory.buildGeometry(polys.subList(rangeStart, rangeEnd));
        Geometry rangeUnion = rangeInOne.buffer(0);
        results.add(rangeUnion);
        progressNum += rangeEnd - rangeStart;
      } catch (Exception e) {
        LOG.warn("Exception in merging "+(rangeEnd - rangeStart)+" polygons", e);
        // Fall back to the union operation
        if (rangeEnd - rangeStart < MinimumThreshold) {
          LOG.warn("Error in union "+rangeStart+"-"+rangeEnd);
          // Do the union directly using the old method (union)
          Geometry rangeUnion = geomFactory.buildGeometry(new ArrayList<Geometry>());
          for (int i = rangeStart; i < rangeEnd; i++) {
            try {
              rangeUnion = rangeUnion.union(polys.get(i));
            } catch (Exception e1) {
              // Log the error and skip it to allow the method to finish
              LOG.error("Error computing union", e);
            }
          }
          results.add(rangeUnion);
          progressNum += rangeEnd - rangeStart;
        } else {
          // Further break the range into two subranges
          rangeStarts.push(rangeStart);
          rangeEnds.push((rangeStart + rangeEnd) / 2);
          rangeStarts.push((rangeStart + rangeEnd) / 2);
          rangeEnds.push(rangeEnd);
          progressDen++;
        }
      }
      if (progress != null)
        progress.progress(progressNum/(float)progressDen);
    }

    // Finally, union all the results
    Geometry finalResult = results.remove(results.size() - 1);
    while (!results.isEmpty()) {
      try {
        finalResult = finalResult.union(results.remove(results.size() - 1));
      } catch (Exception e) {
        LOG.error("Error in union", e);
      }
      progressNum++;
      progress.progress(progressNum/(float)progressDen);
    }
    return finalResult;
  }


  /**
   * Union a group of (overlapping) geometries. It runs as follows.
   * <ol>
   *  <li>All polygons are sorted by the x-dimension of their left most point</li>
   *  <li>We run a plane-sweep algorithm that keeps merging polygons in batches of 500 objects</li>
   *  <li>As soon as part of the answer is to the left of the sweep-line, it
   *   is finalized and reported to the output</li>
   *  <li>As the sweep line reaches the far right, all remaining polygons are
   *   merged and the answer is reported</li>
   * </ol>
   * @param geoms
   * @return
   * @throws IOException
   */
  public static int unionGroup(final Geometry[] geoms,
      final Progressable prog, ResultCollector<Geometry> output) throws IOException {
    if (geoms.length == 1) {
      output.collect(geoms[0]);
      return 1;
    }
    // Sort objects by x to increase the chance of merging overlapping objects
    for (Geometry geom : geoms) {
      Coordinate[] coords = geom.getEnvelope().getCoordinates();
      double minx = Math.min(coords[0].x, coords[2].x);
      geom.setUserData(minx);
    }

    Arrays.sort(geoms, new Comparator<Geometry>() {
      @Override
      public int compare(Geometry o1, Geometry o2) {
        Double d1 = (Double) o1.getUserData();
        Double d2 = (Double) o2.getUserData();
        if (d1 < d2) return -1;
        if (d1 > d2) return +1;
        return 0;
      }
    });
    LOG.debug("Sorted "+geoms.length+" geometries by x");

    final int MaxBatchSize = 500;
    // All polygons that are to the right of the sweep line
    List<Geometry> nonFinalPolygons = new ArrayList<Geometry>();
    int resultSize = 0;

    long reportTime = 0;
    int i = 0;
    while (i < geoms.length) {
      int batchSize = Math.min(MaxBatchSize, geoms.length - i);
      for (int j = 0; j < batchSize; j++) {
        nonFinalPolygons.add(geoms[i++]);
      }
      double sweepLinePosition = (Double)nonFinalPolygons.get(nonFinalPolygons.size() - 1).getUserData();
      Geometry batchUnion;
      batchUnion = safeUnion(nonFinalPolygons, new Progressable.NullProgressable() {
        @Override
        public void progress() { prog.progress(); }
      });
      if (prog != null)
        prog.progress();

      nonFinalPolygons.clear();
      if (batchUnion instanceof GeometryCollection) {
        GeometryCollection coll = (GeometryCollection) batchUnion;
        for (int n = 0; n < coll.getNumGeometries(); n++) {
          Geometry geom = coll.getGeometryN(n);
          Coordinate[] coords = geom.getEnvelope().getCoordinates();
          double maxx = Math.max(coords[0].x, coords[2].x);
          if (maxx < sweepLinePosition) {
            // This part is finalized
            resultSize++;
            if (output != null)
              output.collect(geom);
          } else {
            nonFinalPolygons.add(geom);
          }
        }
      } else {
        nonFinalPolygons.add(batchUnion);
      }

      long currentTime = System.currentTimeMillis();
      if (currentTime - reportTime > 60*1000) { // Report every one minute
        if (prog != null) {
          float p = i / (float)geoms.length;
          prog.progress(p);
        }
        reportTime =  currentTime;
      }
    }

    // Combine all polygons together to produce the answer
    if (output != null) {
      for (Geometry finalPolygon : nonFinalPolygons)
        output.collect(finalPolygon);
    }
    resultSize += nonFinalPolygons.size();

    return resultSize;
  }

  /**
   * Computes the union of multiple groups of polygons. The algorithm runs in
   * the following steps.
   * <ol>
   *  <li>The polygons are flattened using {@link #flattenGeometries(Geometry[])}
   *   to extract the simplest form of them</li>
   *  <li>Polygons are grouped into groups of overlapping polygons using
   *  {@link #groupPolygons(Geometry[], Progressable)} so that we
   *   can compute the answer of each group separately</li>
   *  <li>The union of each group is computed using the {@link #unionGroup(Geometry[], Progressable, ResultCollector)}
   *   function</li>
   * </ol>
   * @param geoms
   * @param prog
   * @param output
   * @return
   * @throws IOException
   */
  public static int multiUnion(Geometry[] geoms, final Progressable prog,
      ResultCollector<Geometry> output) throws IOException {
    final Geometry[] basicShapes = flattenGeometries(geoms);
    prog.progress();

    final Geometry[][] groups = groupPolygons(basicShapes, prog);
    prog.progress();

    int resultSize = 0;
    for (Geometry[] group : groups) {
      resultSize += unionGroup(group, prog, output);
      prog.progress();
    }

    return resultSize;
  }

  public static long spatialJoinLocal(Path[] inFiles, Path outFile, OperationsParams params) throws IOException, InterruptedException {
      // Read the inputs and store them in memory
      List<Shape>[] datasets = new List[inFiles.length];
      final SpatialInputFormat3<Rectangle, Shape> inputFormat =
          new SpatialInputFormat3<Rectangle, Shape>();
      for (int i = 0; i < inFiles.length; i++) {
          datasets[i] = new ArrayList<Shape>();
          FileSystem inFs = inFiles[i].getFileSystem(params);
          Job job = Job.getInstance(params);
          SpatialInputFormat3.addInputPath(job, inFiles[i]);
          for (InputSplit split : inputFormat.getSplits(job)) {
              FileSplit fsplit = (FileSplit) split;
              RecordReader<Rectangle, Iterable<Shape>> reader =
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
                      datasets[i].add(shape.clone());
                  }
              }
              reader.close();
          }
      }

      // Apply the spatial join algorithm
      ResultCollector2<Shape, Shape> output = null;
      PrintStream out = null;
      if (outFile != null) {
          FileSystem outFS = outFile.getFileSystem(params);
          out = new PrintStream(outFS.create(outFile));
          final PrintStream outout = out;
          output = new ResultCollector2<Shape, Shape>() {
              @Override
              public void collect(Shape r, Shape s) {
                  outout.println(r.toText(new Text())+","+s.toText(new Text()));
              }
          };
      }
      long resultCount = SpatialJoin_planeSweep(datasets[0], datasets[1], output, null);

      if (out != null)
          out.close();

      return resultCount;
  }

  public static class DistanceAndPair implements Writable, Cloneable, Comparable<DistanceAndPair> {
    public Double distance;
    public PairWritable<Point> pair = new PairWritable<Point>();

    public DistanceAndPair() {
    }

    public DistanceAndPair(double d, Point a, Point b) {
      distance = d;
      pair.first = a;
      pair.second = b;
    }

    public DistanceAndPair(DistanceAndPair other) {
      this.copy(other);
    }

    public void copy(DistanceAndPair other) {
      distance = other.distance;
      pair.first = other.pair.first;
      pair.second = other.pair.second;
    }

    public void set(double distance, Point refo, Point curo){
      distance = refo.distanceTo(curo);
      pair.first = refo.clone();
      pair.second = curo.clone();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      distance = in.readDouble();
      pair.first = new Point();
      pair.second = new Point();
      pair.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeDouble(distance);
      pair.write(out);
    }

    @Override
    public String toString() {
      StringBuffer str = new StringBuffer();
      str.append(distance);
      str.append("\t");
      str.append(pair.first);
      str.append("\t");
      str.append(pair.second);
      str.append("\t");
      return str.toString();
    }

    @Override
    public int compareTo(DistanceAndPair o) {
      // TODO Auto-generated method stub
      return distance.compareTo(o.distance);
    }

    @Override
    public DistanceAndPair clone() {
      DistanceAndPair c = new DistanceAndPair();
      c.distance = this.distance;
      c.pair.first = pair.first.clone();
      c.pair.second = pair.second.clone();
      return c;
    }
  }

  /**
   * Keeps KNN objects ordered by their distance descending
   *
   * @author Ahmed Eldawy
   *
   */
  public static class KCPObjects<S extends Comparable<S>> extends org.apache.hadoop.util.PriorityQueue<S> {
    /**
     * A hashset of all elements currently in the heap. Used to avoid
     * inserting the same object twice.
     */
    Set<S> allElements = new HashSet<S>();
    /** Capacity of the queue */
    private int capacity;

    private boolean reducePhase = false;

    public KCPObjects(int k) {
      this.capacity = k;
      this.reducePhase = true;
      super.initialize(k);
    }

    public KCPObjects(int k, boolean isReducePhase) {
      this.capacity = k;
      this.reducePhase = isReducePhase;
      super.initialize(k);
    }

    /**
     * Keep elements sorted in descending order (Max heap)
     */
    @Override
    protected boolean lessThan(Object a, Object b) {
      return ((S) a).compareTo((S) b) > 0;
    }

    @Override
    public boolean insert(S newElement) {
      // Skip element if already there
      if (reducePhase && allElements.contains(newElement)) {
        // LOG.info("Ya existe:"+newElement+": "+allElements);
        return false;
      }
      boolean overflow = this.size() == capacity;
      Object overflowItem = this.top();
      boolean inserted = super.insert(newElement);
      if (reducePhase && inserted) {
        if (overflow)
          allElements.remove(overflowItem);
        allElements.add(newElement);
      }
      return inserted;
    }

    public int getCapacity() {
      return this.capacity;
    }

    public boolean isFull() {
      return this.size()>=this.capacity;
    }
  }

  public static PriorityQueue<DistanceAndPair> SpatialBinaryClosestPair_planeSweep(Shape[] p1, Shape[] q1, int numberk, Double delta, KCPObjects<DistanceAndPair> pq, Reporter reporter) {

    final Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
        if (o1 == null || o2 == null) {
          if (o1 == o2) {
            return 0;
          }
          if (o1 == null) {
            return 1;
          } else {
            return -1;
          }

        }
        if (o1.getMBR().x1 == o2.getMBR().x1)
          return 0;
        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
      }
    };

    LOG.info("ordenando p de longitud " + p1.length);

    Arrays.sort(p1,comparator);

    LOG.info("ordenando q de longitud " + q1.length);
    Arrays.sort(q1,comparator);

    LOG.info("Calculando delta");

    int gtotPoints1 = p1.length-1, gtotPoints2 = q1.length-1;

    for(; gtotPoints1>=0; gtotPoints1--){
      if(p1[gtotPoints1]!=null){
        gtotPoints1++;
        break;
      }
    }

    for(; gtotPoints2>=0; gtotPoints2--){
      if(q1[gtotPoints2]!=null){
        gtotPoints2++;
        break;
      }
    }

    //LOG.info("R "+gtotPoints1+" . S "+gtotPoints2);

    int i = 0, j = 0, k, stk; // local counter of the points
    double dx, dy, ssdx, distance, gdmax = delta != null ? delta : -1;

    KCPObjects<DistanceAndPair> kcpho = pq==null?new KCPObjects<DistanceAndPair>(numberk, false):pq;
    PriorityQueue<DistanceAndPair> ho = kcpho;

    DistanceAndPair aux = new DistanceAndPair();
    Point refo, curo;
    long leftp = -1, leftq = -1;
    Shape[] p = Arrays.copyOf(p1, gtotPoints1 + 1);
    p[gtotPoints1] = new Point(Double.MAX_VALUE, Double.MAX_VALUE);
    Shape[] q = Arrays.copyOf(q1, gtotPoints2 + 1);
    q[gtotPoints2] = new Point(Double.MAX_VALUE, Double.MAX_VALUE);
    //LOG.info(gtotPoints1 + ":" + gtotPoints2);

    // first find the most left point of two datasets
    if (((Point) p[0]).x < ((Point) q[0]).x) { // if P[0] < Q[0]
      // if the datasets have no intersection
      if (((Point) p[gtotPoints1 - 1]).x <= ((Point) q[0]).x) { // if
        // P[last]<=Q[0]
        i = gtotPoints1; // the LEFT scan begins from i-1 index
        // j = 0L; // the Q set is on the right side
      }
    } else { // else if the most left point is from set Q
      // if the datasets have no intersection
      if (((Point) q[gtotPoints2 - 1]).x <= ((Point) p[0]).x) { // if
        // Q[last]<=P[0]
        j = gtotPoints2; // the LEFT scan begins from j-1 index
        // i = 0L; // the P set is on the right side
      }
    }

    // if we have intersection between the two datasets
    // 1. while the points of both sets are not finished
    for (;;) {
      if (reporter != null)
        reporter.setStatus("i:" + i + ":j:" + j);

      if (((Point) p[i]).x < ((Point) q[j]).x) { // if the subset is from
        // the set P
        // 2. for each P[i] while i < P.N and P[i].x < Q[j].x increment
        // i
        stk = j - 1;
        do {
          if (reporter != null)
            reporter.setStatus("i:" + i + ":j:" + j);

          // 3. if j-1 = leftq then continue // this run was
          // interrupted
          if (stk == leftq) {
            continue;
          }
          // 4. set ref_point = P[i]
          refo = ((Point) p[i]);
          k = stk;
          // 5. for k=j-1 to leftq decrement k
          do {
            // 6. set cur_point = Q[k]
            curo = ((Point) q[k]);

            if (gdmax == -1) {
              aux.distance = refo.distanceTo(curo);
              aux.pair.second = curo.clone();
              aux.pair.first = refo.clone();
              gdmax = aux.distance;
              ho.insert(aux.clone());
              //LOG.info("Cambiamos ho " + ho.top());
            } else {
              if (kcpho.size() < kcpho.getCapacity()) {
                if (delta == null) {
                  aux.distance = refo.distanceTo(curo);
                  aux.pair.second = curo.clone();
                  aux.pair.first = refo.clone();
                  ho.insert(aux.clone());
                  //LOG.info("Cambiamos ho " + ho.top());
                  gdmax = ho.top().distance;
                } else {
                  // 7. calculate distance
                  // dx=ref_point.x-cur_point.x
                  dx = refo.x - curo.x;
                  // 14. if dx >= maxheap.root.dist then
                  // reduce the leftq limit and break
                  if (dx > gdmax) { // check if it out from
                    // sweeping axis
                    leftq = k; // reduce the left limit for
                    // the
                    // Q set
                    break; // exit k, all other points have
                    // longer distance
                  } // end of if(dx >= gmaxheap.heap[1].dist)
                  // 15.1. calculate the distance
                  // dy=calc_dist_y_axis(ref_point,cur_point)
                  ssdx = dx * dx;
                  dy = curo.y - refo.y;
                  ssdx += dy * dy;
                  if (ssdx <= gdmax * gdmax) {
                    if(reporter!=null)reporter.getCounter(KCPCounters.NUM_OPERATIONS).increment(1);

                    distance = Math.sqrt(ssdx);
                    aux.distance = distance;
                    aux.pair.second = curo.clone();
                    aux.pair.first = refo.clone();
                    ho.insert(aux.clone());
                    //LOG.info("Cambiamos ho " + ho.top());
                  } // end of if(d==NUM_DIM)
                }

              } else {
                // 7. calculate distance
                // dx=ref_point.x-cur_point.x
                dx = refo.x - curo.x;
                // 14. if dx >= maxheap.root.dist then
                // reduce the leftq limit and break
                if (dx >= gdmax) { // check if it out from
                  // sweeping axis
                  leftq = k; // reduce the left limit for the
                  // Q set
                  break; // exit k, all other points have
                  // longer distance
                } // end of if(dx >= gmaxheap.heap[1].dist)
                // 15.1. calculate the distance
                // dy=calc_dist_y_axis(ref_point,cur_point)
                ssdx = dx * dx;
                dy = curo.y - refo.y;
                ssdx += dy * dy;
                if (ssdx < gdmax * gdmax) {
                  if(reporter!=null)reporter.getCounter(KCPCounters.NUM_OPERATIONS).increment(1);

                  distance = Math.sqrt(ssdx);
                  aux.distance = distance;
                  aux.pair.second = curo.clone();
                  aux.pair.first = refo.clone();
                  ho.insert(aux.clone());
                  gdmax = ho.top().distance;
                  //LOG.info("Cambiamos ho " + ho.top());
                } // end of if(d==NUM_DIM)

              }
            }
          } while (--k > leftq); // next point of the set Q
        } while ((((Point) p[++i]).x < ((Point) q[j]).x)); // next i :
        // next
        // point
        // from the
        // set P if
        // the run
        // continues

      } else if (j < gtotPoints2) { // else if the set Q is not finished
        ((Point) p[gtotPoints1]).x = ((Point) q[gtotPoints2 - 1]).x + 1;

        // 18. for each Q[j] while j < Q.N and Q[j] <= P[i] increment j
        stk = i - 1;
        do {
          if (reporter != null)
            reporter.setStatus("i:" + i + ":j:" + j);

          // 19. if i-1 = leftp then continue // this run was
          // interrupted
          if (stk == leftp) {
            continue;
          }
          // 20. set ref_point = Q[j]
          refo = ((Point) q[j]);
          k = stk;
          // 21. for k=i-1 to leftp decrement k
          do {
            // 22. set cur_point = P[k]
            curo = ((Point) p[k]);

            if (gdmax == -1) {
              aux.distance = refo.distanceTo(curo);
              aux.pair.second = curo.clone();
              aux.pair.first = refo.clone();
              gdmax = aux.distance;
              ho.insert(aux.clone());
              //LOG.info("Cambiamos ho " + ho.top());
            } else {
              if (kcpho.size() < kcpho.getCapacity()) {
                if (delta == null) {
                  aux.distance = refo.distanceTo(curo);
                  aux.pair.second = curo.clone();
                  aux.pair.first = refo.clone();
                  ho.insert(aux.clone());
                  //LOG.info("Cambiamos ho " + ho.top());
                  gdmax = ho.top().distance;
                } else {
                  // 7. calculate distance
                  // dx=ref_point.x-cur_point.x
                  dx = refo.x - curo.x;
                  // 14. if dx >= maxheap.root.dist then
                  // reduce the leftq limit and break
                  if (dx > gdmax) { // check if it out from
                    // sweeping axis
                    leftp = k; // reduce the left limit for
                    // the
                    // Q set
                    break; // exit k, all other points have
                    // longer distance
                  } // end of if(dx >= gmaxheap.heap[1].dist)
                  // 15.1. calculate the distance
                  // dy=calc_dist_y_axis(ref_point,cur_point)
                  ssdx = dx * dx;
                  dy = curo.y - refo.y;
                  ssdx += dy * dy;
                  if (ssdx <= gdmax * gdmax) {
                    if(reporter!=null)reporter.getCounter(KCPCounters.NUM_OPERATIONS).increment(1);

                    distance = Math.sqrt(ssdx);
                    aux.distance = distance;
                    aux.pair.second = curo.clone();
                    aux.pair.first = refo.clone();
                    ho.insert(aux.clone());
                    //LOG.info("Cambiamos ho " + ho.top());
                  } // end of if(d==NUM_DIM)
                }

              } else {
                // 7. calculate distance
                // dx=ref_point.x-cur_point.x
                dx = refo.x - curo.x;
                // 14. if dx >= maxheap.root.dist then
                // reduce the leftq limit and break
                if (dx >= gdmax) { // check if it out from
                  // sweeping axis
                  leftp = k; // reduce the left limit for the
                  // Q set
                  break; // exit k, all other points have
                  // longer distance
                } // end of if(dx >= gmaxheap.heap[1].dist)
                // 15.1. calculate the distance
                // dy=calc_dist_y_axis(ref_point,cur_point)
                ssdx = dx * dx;
                dy = curo.y - refo.y;
                ssdx += dy * dy;
                if (ssdx < gdmax * gdmax) {
                  if(reporter!=null)reporter.getCounter(KCPCounters.NUM_OPERATIONS).increment(1);

                  distance = Math.sqrt(ssdx);
                  aux.distance = distance;
                  aux.pair.second = curo.clone();
                  aux.pair.first = refo.clone();
                  ho.insert(aux.clone());
                  gdmax = ho.top().distance;
                  //LOG.info("Cambiamos ho " + ho.top());
                } // end of if(d==NUM_DIM)

              }
            }
          } while (--k > leftp); // next point of the set P
        } while (((Point) q[++j]).x <= ((Point) p[i]).x); // next j :
        // next
        // point
        // from the
        // set Q if
        // the run
        // continues
        // revert the max value in the P[last].x
        ((Point) p[gtotPoints1]).x = ((Point) q[gtotPoints2]).x;
        // P[gtotPoints1].m[0]=Q[gtotPoints2].m[0];
      } else {
        break; // the process is finished
      }

    } // loop while(i < gtotPoints1 || j < gtotPoints2)

    return ho;
  }

  public static PriorityQueue<DistanceAndPair> SpatialBinaryClosestPair_planeSweepClassic(Shape[] p, Shape[] q, int numberk, Double delta, KCPObjects<DistanceAndPair> pq, Reporter reporter) {

    int i = 0, j = 0, k, stk; // local counter of the points
    double dx, dy, ssdx, distance, gdmax = delta != null ? delta : -1;

    final Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
        if (o1 == null || o2 == null) {
          if (o1 == o2) {
            return 0;
          }
          if (o1 == null) {
            return 1;
          } else {
            return -1;
          }

        }
        if (o1.getMBR().x1 == o2.getMBR().x1)
          return 0;
        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
      }
    };

    LOG.info("ordenando p de longitud " + p.length);

    Arrays.sort(p,comparator);

    LOG.info("ordenando q de longitud " + q.length);
    Arrays.sort(q,comparator);

    LOG.info("Calculando delta");

    KCPObjects<DistanceAndPair> kcpho = pq==null?new KCPObjects<DistanceAndPair>(numberk, false):pq;
    PriorityQueue<DistanceAndPair> ho = kcpho;
    Point refo, curo;
    int gtotPoints1 = p.length, gtotPoints2 = q.length;
    DistanceAndPair aux = new DistanceAndPair();

    while (i < gtotPoints1 && p[i] != null && j < gtotPoints2 && q[j] != null) {

      if (reporter != null)
        reporter.setStatus("i:" + i + ":j:" + j);

      if (((Point) p[i]).x < ((Point) q[j]).x) { // if P[i] < Q[j] -- m[0]
        // coordenada x
        // i is constant and k begins from j for all points in Q
        refo = ((Point) p[i]);
        for (k = j; k < gtotPoints2 && q[k]!=null; k++) {
          curo = ((Point) q[k]);
          if (gdmax == -1) {
            aux.distance = refo.distanceTo(curo);
            aux.pair.second = curo.clone();
            aux.pair.first = refo.clone();
            gdmax = aux.distance;
            ho.insert(aux.clone());
            // LOG.info("Cambiamos ho " + ho.top());
          } else {
            if (kcpho.size() < kcpho.getCapacity()) {
              if (delta == null) {
                aux.distance = refo.distanceTo(curo);
                aux.pair.second = curo.clone();
                aux.pair.first = refo.clone();
                ho.insert(aux.clone());
                // LOG.info("Cambiamos ho " + ho.top());
                gdmax = ho.top().distance;
              } else {
                dx = curo.x - refo.x; // the dx = Q[0]-P[0]
                if (dx > gdmax) { // check if it out from
                  // sweeping
                  // axis
                  break; // exit k, all other points have
                  // longer
                  // distance
                }
                if (reporter != null)
                  reporter.getCounter(KCPCounters.NUM_OPERATIONS).increment(1);
                distance = refo.distanceTo(curo);
                if (distance <= gdmax) {
                  aux.distance = distance;
                  aux.pair.second = curo.clone();
                  aux.pair.first = refo.clone();
                  ho.insert(aux.clone());
                  // LOG.info("Cambiamos ho " + ho.top());
                }
              }

            } else {
              dx = curo.x - refo.x; // the dx = Q[0]-P[0]
              if (dx >= gdmax) { // check if it out from sweeping
                // axis
                break; // exit k, all other points have longer
                // distance
              }
              if (reporter != null)
                reporter.getCounter(KCPCounters.NUM_OPERATIONS).increment(1);
              distance = refo.distanceTo(curo);
              if (distance < gdmax) {
                aux.distance = distance;
                aux.pair.second = curo.clone();
                aux.pair.first = refo.clone();
                ho.insert(aux.clone());
                // LOG.info("Cambiamos ho " + ho.top());
                gdmax = ho.top().distance;
              }
            }
          }
        } // next k

        i++;

      } else {
        // j is constant and k begins from i for all points in P
        refo = ((Point) q[j]);
        for (k = i; k < gtotPoints1 && p[k]!=null; k++) {
          curo = ((Point) p[k]);
          if (gdmax == -1) {
            aux.distance = refo.distanceTo(curo);
            aux.pair.second = curo.clone();
            aux.pair.first = refo.clone();
            gdmax = aux.distance;
            ho.insert(aux.clone());
            // LOG.info("Cambiamos ho " + ho.top());
          } else {
            if (kcpho.size() < kcpho.getCapacity()) {
              if (delta == null) {
                aux.distance = refo.distanceTo(curo);
                aux.pair.second = curo.clone();
                aux.pair.first = refo.clone();
                ho.insert(aux.clone());
                // LOG.info("Cambiamos ho " + ho.top());
                gdmax = ho.top().distance;
              } else {
                dx = curo.x - refo.x; // the dx = P[0]-Q[0]
                if (dx > gdmax) { // check if it out from
                  // sweeping
                  // axis
                  break; // exit k, all other points have
                  // longer
                  // distance
                }
                if (reporter != null)
                  reporter.getCounter(KCPCounters.NUM_OPERATIONS).increment(1);
                distance = refo.distanceTo(curo);
                if (distance <= gdmax) {
                  aux.distance = distance;
                  aux.pair.second = curo.clone();
                  aux.pair.first = refo.clone();
                  ho.insert(aux.clone());
                  // LOG.info("Cambiamos ho " + ho.top());
                }
              }

            } else {
              dx = curo.x - refo.x; // the dx = P[0]-Q[0]
              if (dx >= gdmax) { // check if it out from sweeping
                // axis
                break; // exit k, all other points have longer
                // distance
              }
              if (reporter != null)
                reporter.getCounter(KCPCounters.NUM_OPERATIONS).increment(1);
              distance = refo.distanceTo(curo);
              if (distance < gdmax) {
                aux.distance = distance;
                aux.pair.second = curo.clone();
                aux.pair.first = refo.clone();
                ho.insert(aux.clone());
                gdmax = ho.top().distance;
                // LOG.info("Cambiamos ho " + ho.top());
              }
            }
          }
        } // next k

        j++;

      }
    } // loop while (i < gtotPoints1 && j < gtotPoints2) }

    return ho;

  }

}
