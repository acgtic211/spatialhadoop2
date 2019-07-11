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
import edu.umn.cs.spatialHadoop.core.*;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms.KCPObjects;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.indexing.RTree;
import edu.umn.cs.spatialHadoop.mapred.*;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.PriorityQueue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

/**
 * Performs a spatial join between two or more files using the redistribute-join
 * algorithm.
 *
 * @author Ahmed Eldawy
 *
 */
public class KClosestPairsQueryMbr {
	private static final Log LOG = LogFactory.getLog(KClosestPairsQueryMbr.class);
	private static RunningJob lastRunningJob;
	private static int maxBytesInOneRead = 1024 * 1024 * 100;
	private static int maxShapesInOneRead = 1000000;
	private static boolean isOneShotReadMode = false;

	private static final NullWritable Dummy = NullWritable.get();

	public static class DistanceAndPairGeneric<S extends Shape> implements Writable, Cloneable, Comparable<DistanceAndPairGeneric<S>> {
		public Double distance;
		public PairWritable<S> pair = new PairWritable<S>();
		private Class<? extends S> cls;

		public DistanceAndPairGeneric() {
		}

		public DistanceAndPairGeneric(Class<? extends S> pCls) {
			this.cls = pCls;
		}


		public DistanceAndPairGeneric(double d, S a, S b) {
			distance = d;
			pair.first = a;
			pair.second = b;
		}

		public DistanceAndPairGeneric(DistanceAndPairGeneric<S> other) {
			this.copy(other);
		}

		public void copy(DistanceAndPairGeneric<S> other) {
			distance = other.distance;
			pair.first = other.pair.first;
			pair.second = other.pair.second;
		}

		public void set(double distance, S refo, S curo){
			this.distance = distance;
			pair.first = (S)refo.clone();
			pair.second = (S)curo.clone();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			String className = in.readUTF();
			distance = in.readDouble();
			try {
				pair.first = (S)Class.forName(className).newInstance();
				pair.second = (S)Class.forName(className).newInstance();
			} catch (Exception e) {
				e.printStackTrace();
			}
			pair.readFields(in);

		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.cls.getName());
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
		public int compareTo(DistanceAndPairGeneric o) {
			// TODO Auto-generated method stub
			return distance.compareTo(o.distance);
		}

		@Override
		public DistanceAndPairGeneric clone() {
			DistanceAndPairGeneric c = new DistanceAndPairGeneric(cls);
			c.distance = this.distance;
			c.pair.first = pair.first.clone();
			c.pair.second = pair.second.clone();
			return c;
		}
	}

	private static Double calculateDistance(Shape refo, Shape curo, double gdmax, boolean full, int algorithm, Reporter reporter) {
		return calculateDistance(refo.getMBR(),curo.getMBR(),gdmax,full,algorithm,reporter);
	}

	private static Double calculateDistance(Rectangle r1, Rectangle r2, double gdmax, boolean full, int algorithm, Reporter reporter){

		double dx, dy, ssdx, gdmax2;
		Double distance;
		int realAlgorithm = (algorithm-1) % 3;


		dx = 0;
		if (r2.x1 > r1.x2)
			dx = r2.x1 - r1.x2;
		else if (r1.x1 > r2.x2)
			dx = r1.x1 - r2.x2;


		if (dx > gdmax || (dx == gdmax && full)) {
			return null;
		}

		if(realAlgorithm==0) {
			if (reporter != null)
				reporter.getCounter(SpatialAlgorithms.KCPCounters.NUM_OPERATIONS).increment(1);
			distance = r1.getMinDistance(r2);
			return distance;
		}

		dy = 0;
		if (r2.y1 > r1.y2)
			dy = r2.y1 - r1.y2;
		else if (r1.y1 > r2.y2)
			dy = r1.y1 - r2.y2;

		if (dy > gdmax || (dy == gdmax && full)) {
			distance = -1.0d;
			return distance;
		}

		if(realAlgorithm==1) {
			if (reporter != null)
				reporter.getCounter(SpatialAlgorithms.KCPCounters.NUM_OPERATIONS).increment(1);
			distance = r1.getMinDistance(r2);
			return distance;
		}

		ssdx = dx * dx;
		ssdx += dy * dy;
		gdmax2 = gdmax * gdmax;
		if (ssdx < gdmax2 || (ssdx == gdmax && !full)) {
			if (reporter != null)
				reporter.getCounter(SpatialAlgorithms.KCPCounters.NUM_OPERATIONS).increment(1);
			distance = Math.sqrt(ssdx);
			return distance;
		}

		return Double.MAX_VALUE;

	}


  /**
   *
   * Finds Top K Closest Pairs using Reverse Plain Sweep technique
   *
   * @param p Ordered Shape array
   * @param q Ordered Shape array
   * @param numberK Number of elements needed for Top k
   * @param algorithm Number of pruning algorithm (classic, rectangle, circle)
   * @param reporter Reporter to count number of full calculations
   * @return Top K Closest Pairs
   */
	public static PriorityQueue<DistanceAndPairGeneric<Shape>> reverseKCPQuery(Shape[] p, Shape[] q, Integer numberK, int algorithm,
                                                               final Reporter reporter) {

		return reverseKCPQuery(p, q, numberK, algorithm, null, null, reporter);
	}

  /**
   *
   * Finds Top K Closest Pairs using Reverse Plain Sweep technique
   *
   * @param p1 Ordered Shape array
   * @param q1 Ordered Shape array
   * @param numberK Number of elements needed for Top k
   * @param algorithm Number of pruning algorithm (classic, rectangle, circle)
   * @param beta Pre-filtered beta value
   * @param alpha alpha allowance value
   * @param reporter Reporter to count number of full calculations
   * @return Top K Closest Pairs
   */
	public static PriorityQueue<DistanceAndPairGeneric<Shape>> reverseKCPQuery(Shape[] p1, Shape[] q1, Integer numberK, int algorithm, Double beta, Float alpha,
                                                               final Reporter reporter) {

		int i = 0, j = 0, k, stk; // local counter of the points
		double dx, gdmax = beta != null ? beta : -1;
		Double distance;
		float fAlpha = alpha != null ? alpha : 0.0f;
		KCPObjects<DistanceAndPairGeneric<Shape>> kcpho = new KCPObjects<DistanceAndPairGeneric<Shape>>(numberK, false);
		PriorityQueue<DistanceAndPairGeneric<Shape>> ho = kcpho;
		DistanceAndPairGeneric<Shape> aux = new DistanceAndPairGeneric<Shape>(p1[0].getClass());
		Rectangle refo, curo;
		long leftp = -1, leftq = -1;
		int gtotPoints1 = p1.length, gtotPoints2 = q1.length;
		Shape[] p = Arrays.copyOf(p1, gtotPoints1 + 1);
		p[gtotPoints1] = new Point(Double.MAX_VALUE, Double.MAX_VALUE);
		Shape[] q = Arrays.copyOf(q1, gtotPoints2 + 1);
		q[gtotPoints2] = new Point(Double.MAX_VALUE, Double.MAX_VALUE);
		LOG.info(gtotPoints1 + ":" + gtotPoints2);

		// first find the most left point of two datasets
		if (p[0].getMBR().x1 < q[0].getMBR().x1) { // if P[0] < Q[0]
			// if the datasets have no intersection
			if (p[gtotPoints1 - 1].getMBR().x2 <= q[0].getMBR().x1) { // if
																		// P[last]<=Q[0]
				i = gtotPoints1; // the LEFT scan begins from i-1 index
				// j = 0L; // the Q set is on the right side
			}
		} else { // else if the most left point is from set Q
			// if the datasets have no intersection
			if (q[gtotPoints2 - 1].getMBR().x2 <= p[0].getMBR().x1) { // if
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

			if (p[i].getMBR().x1 < q[j].getMBR().x1) { // if the subset is from
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
					refo = p[i].getMBR();
					k = stk;
					// 5. for k=j-1 to leftq decrement k
					do {
						// 6. set cur_point = Q[k]
						curo = q[k].getMBR();

						if (!kcpho.isFull() && beta == null) {

							distance = refo.getMinDistance(curo);
							aux.set(distance, refo, curo);
							ho.insert(aux.clone());
							gdmax = ho.top().distance*(1.0f-fAlpha);

						} else {

							distance = calculateDistance(refo,curo,gdmax,kcpho.isFull(),algorithm, reporter);

							if(distance==null){
								leftq = k;
								break;
							}else if(distance<0){
								continue;
							}

							if (distance < gdmax || (distance == gdmax && !kcpho.isFull())) {
								aux.set(distance, refo, curo);
								ho.insert(aux.clone());
								gdmax = ho.top().distance * (1.0f - fAlpha);
							}

						}

					} while (--k > leftq); // next point of the set Q
				} while (p[++i].getMBR().x1 < q[j].getMBR().x1); // next i :
																	// next
																	// point
																	// from the
																	// set P if
																	// the run
																	// continues

			} else if (j < gtotPoints2) { // else if the set Q is not finished
				((Point) p[gtotPoints1]).x = q[gtotPoints2 - 1].getMBR().x2 + 1;

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
					refo = q[j].getMBR();
					k = stk;
					// 21. for k=i-1 to leftp decrement k
					do {
						// 22. set cur_point = P[k]
						curo = p[k].getMBR();
						if (!kcpho.isFull() && beta == null) {

							distance = refo.getMinDistance(curo);
							aux.set(distance, refo, curo);
							ho.insert(aux.clone());
							gdmax = ho.top().distance*(1.0f-fAlpha);

						} else {

							distance = calculateDistance(refo,curo,gdmax,kcpho.isFull(),algorithm, reporter);

							if(distance==null){
								leftp = k;
								break;
							}else if(distance<0){
								continue;
							}

							if (distance < gdmax || (distance == gdmax && !kcpho.isFull())) {
								aux.set(distance, refo, curo);
								ho.insert(aux.clone());
								gdmax = ho.top().distance * (1.0f - fAlpha);
							}

						}

					} while (--k > leftp); // next point of the set P
				} while (q[++j].getMBR().x1 <= p[i].getMBR().x1); // next j :
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

  /**
   * Filters pair of cells based on beta value
   */
	public static class BinaryClosestPairFilter extends DefaultBlockFilter {

		private double beta = -1;
		private int numberk;

		@Override
		public void configure(Configuration conf) {
			super.configure(conf);
			beta = Double.parseDouble(conf.get("beta", "-1"));
			numberk = conf.getInt("k", 1);
		}

		@Override
		public void selectCellPairs(final GlobalIndex<Partition> gIndex1, final GlobalIndex<Partition> gIndex2,
                                final ResultCollector2<Partition, Partition> output) {

			// TODO PLANE SWEEP
			for (Partition r : gIndex1) {
				for (Partition s : gIndex2) {
					double minDistance = r.getMinDistance(s);
					LOG.info("minDistance > beta? " + minDistance + " : " + beta);
					if (beta != -1 && minDistance > beta) {
						LOG.info("Skipping partitions " + r + ", " + s);
					} else {

						output.collect(r, s);

					}
				}
			}
		}
	}

  /**
   * Top K Closest Pairs Query Map Phase
   */
	private static class BinaryClosestPairMap extends MapReduceBase implements
			Mapper<PairWritable<Rectangle>, PairWritable<? extends Writable>, NullWritable, DistanceAndPairGeneric<Shape>> {

		private Double beta = -1.;
		private int numberK;
		private int algorithm;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			beta = Double.parseDouble(job.get("beta", "-1"));
			algorithm = job.getInt("alg", 1);
			numberK = job.getInt("k", 1);
		}

		public void map(final PairWritable<Rectangle> key, final PairWritable<? extends Writable> value,
                    final OutputCollector<NullWritable, DistanceAndPairGeneric<Shape>> output, final Reporter reporter)
						throws IOException {

			if (value.first instanceof RTree && value.second instanceof RTree) {
				// Join two R-trees

        /*RTree<Shape> r1 = (RTree<Shape>) value.first;

        RTree<Shape> r2 = (RTree<Shape>) value.second;

				RTree.spatialBinaryClosestPair(r1, r2, numberK, beta,
						new ResultCollector2<NullWritable, DistanceAndPair>() {
							@Override
							public void collect(NullWritable r, DistanceAndPair s) {
								try {
									output.collect(r, s);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}

						}, reporter);*/

			} else if (value.first instanceof ArrayWritable && value.second instanceof ArrayWritable) {
				Shape[] p = (Shape[]) ((ArrayWritable) value.first).get();
				Shape[] q = (Shape[]) ((ArrayWritable) value.second).get();

				Arrays.sort(p);

				Arrays.sort(q);

				PriorityQueue<DistanceAndPairGeneric<Shape>> out = null;
				switch (algorithm) {
					case 1:
					case 2:
					case 3:
					case 4:
					case 5:
					case 6:
						out = reverseKCPQuery(p, q, numberK, algorithm, beta, null, reporter);
						break;
					default:
						break;
				}

				if (out == null) {
					LOG.error("Error null closest pair for input of size: " + p.length + " " + q.length);
				}

				LOG.info("Found the closest pair: " + out.size());

				if (out.size() == numberK) {
					double deltaAux = out.top().distance;
					if (deltaAux < beta || beta == -1) {
						beta = deltaAux;
					}
				}

				DistanceAndPairGeneric<Shape> t = new DistanceAndPairGeneric<Shape>();
				while (out.size() > 0) {
					DistanceAndPairGeneric<Shape> aux = out.pop();
					t.set(aux.distance,aux.pair.first,aux.pair.second);
					t.cls = aux.pair.first.getClass();
					output.collect(Dummy, t);
				}

			} else {
				throw new RuntimeException(
						"Cannot join " + value.first.getClass() + " with " + value.second.getClass());
			}
			reporter.progress();
		}
	}

  /**
   * Top K Closest Pairs Query Reduce Phase
   */
	public static class Reduce extends MapReduceBase
			implements Reducer<NullWritable, DistanceAndPairGeneric<Shape>, NullWritable, DistanceAndPairGeneric<Shape>> {

		private int numberK;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			numberK = job.getInt("k", 1);
		}

		@Override
		public void reduce(NullWritable key, Iterator<DistanceAndPairGeneric<Shape>> values,
				OutputCollector<NullWritable, DistanceAndPairGeneric<Shape>> output, Reporter reporter) throws IOException {

			PriorityQueue<DistanceAndPairGeneric<Shape>> closest = new KCPObjects<DistanceAndPairGeneric<Shape>>(numberK);
			while (values.hasNext()) {
				DistanceAndPairGeneric<Shape> actual = values.next();
				closest.insert(actual.clone());
			}

			DistanceAndPairGeneric<Shape>[] kcpAscendingOrder = new DistanceAndPairGeneric[closest.size()];
			int i = kcpAscendingOrder.length;

			while (closest.size() > 0) {
				DistanceAndPairGeneric<Shape> t = closest.pop();
				kcpAscendingOrder[--i] = t;
			}
			// Write results in the ascending order
			for (DistanceAndPairGeneric<Shape> t : kcpAscendingOrder) {
				output.collect(Dummy, t);
			}
		}


	}

	/**
	 * Input format that returns a record reader that reads a pair of arrays of
	 * shapes
	 *
	 * @author Ahmed Eldawy
	 *
	 */
	public static class KCPQInputFormatArray extends BinarySpatialInputFormat<Rectangle, ArrayWritable> {

		/**
		 * Reads a pair of arrays of shapes
		 *
		 * @author Ahmed Eldawy
		 *
		 */
		public static class KCPQRecordReader extends BinaryRecordReader<Rectangle, ArrayWritable> {
			public KCPQRecordReader(Configuration conf, CombineFileSplit fileSplits) throws IOException {
				super(conf, fileSplits);
			}

			@Override
			protected RecordReader<Rectangle, ArrayWritable> createRecordReader(Configuration conf,
                                                                          CombineFileSplit split, int i) throws IOException {
				FileSplit fsplit = new FileSplit(split.getPath(i), split.getStartOffsets()[i], split.getLength(i),
						split.getLocations());
				return new ShapeArrayRecordReader(conf, fsplit);
			}
		}

		@Override
		public RecordReader<PairWritable<Rectangle>, PairWritable<ArrayWritable>> getRecordReader(InputSplit split,
                                                                                              JobConf job, Reporter reporter) throws IOException {
			reporter.progress();
			return new KCPQRecordReader(job, (CombineFileSplit) split);
		}
	}

	/**
	 * Input format that returns a record reader that reads a pair of arrays of
	 * shapes
	 *
	 * @author Ahmed Eldawy
	 *
	 */
	public static class KCQPInputFormatRTree<S extends Shape>
			extends BinarySpatialInputFormat<Rectangle, RTree<S>> {

		/**
		 * Reads a pair of arrays of shapes
		 *
		 * @author Ahmed Eldawy
		 *
		 */
		public static class KCPQRecordReader<S extends Shape> extends BinaryRecordReader<Rectangle, RTree<S>> {
			public KCPQRecordReader(Configuration conf, CombineFileSplit fileSplits) throws IOException {
				super(conf, fileSplits);
			}

			@Override
			protected RecordReader<Rectangle, RTree<S>> createRecordReader(Configuration conf, CombineFileSplit split,
                                                                     int i) throws IOException {
				FileSplit fsplit = new FileSplit(split.getPath(i), split.getStartOffsets()[i], split.getLength(i),
						split.getLocations());
				return new RTreeRecordReader<S>(conf, fsplit);
			}
		}

		@Override
		public RecordReader<PairWritable<Rectangle>, PairWritable<RTree<S>>> getRecordReader(InputSplit split,
                                                                                         JobConf job, Reporter reporter) throws IOException {
			reporter.progress();
			return new KCPQRecordReader<S>(job, (CombineFileSplit) split);
		}
	}

	/**
	 * Performs a redistribute Top K Closest Pairs query between the given files using a
	 * plain sweep join algorithm. Currently, we only support a pair of files.
	 *
	 * @param inFiles Input files
	 * @param userOutputPath User output file
   * @param params Diferent configuration params
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static <S extends Shape> long kcpqStep(Path[] inFiles, Path userOutputPath, OperationsParams params)
			throws IOException, InterruptedException {
		long t1 = System.currentTimeMillis();

		JobConf job = new JobConf(params, KClosestPairsQueryMbr.class);

		FileSystem fs[] = new FileSystem[inFiles.length];
		for (int i_file = 0; i_file < inFiles.length; i_file++)
			fs[i_file] = inFiles[i_file].getFileSystem(job);

		Path outputPath = userOutputPath;
		if (outputPath == null) {
			do {
				outputPath = new Path(inFiles[0].getName() + ".kcpq_" + (int) (Math.random() * 1000000));
			} while (fs[0].exists(outputPath));
		}

		job.setJobName("KCPQuery");
		//ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();

		LOG.info("Joining " + inFiles[0] + " X " + inFiles[1]);

		int notFiltered = params.getInt("filtered", 2);


		if(notFiltered==0){
			job.set("beta", "-1");
		}else{
			DistanceAndPairGeneric<Shape> beta;
			if(notFiltered==2)
				beta =	calculateSampleThreshold2(fs,inFiles, t1, job);
			else
				beta = calculateSampleThreshold(inFiles, t1, job);
			job.set("beta", beta.distance + "");
		}

		if (SpatialSite.isRTree(fs[0], inFiles[0]) && SpatialSite.isRTree(fs[1], inFiles[1])) {
			job.setInputFormat(KCQPInputFormatRTree.class);
		} else {
			if (isOneShotReadMode) {
				// Ensure all objects are read in one shot
				job.setInt(SpatialSite.MaxBytesInOneRead, -1);
				job.setInt(SpatialSite.MaxShapesInOneRead, -1);
			} else {
				job.setInt(SpatialSite.MaxBytesInOneRead, maxBytesInOneRead);
				job.setInt(SpatialSite.MaxShapesInOneRead, maxShapesInOneRead);
			}
			job.setInputFormat(KCPQInputFormatArray.class);
		}

		// Binary version of spatial join (two different input files)
		job.setClass(SpatialSite.FilterClass, BinaryClosestPairFilter.class, BlockFilter.class);
		FileInputFormat.setInputPaths(job, inFiles);
		job.setMapperClass(BinaryClosestPairMap.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DistanceAndPairGeneric.class);
		job.setReducerClass(Reduce.class);
		//job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
		job.setNumReduceTasks(1); // One reduce needed for this task

		if (job.getBoolean("output", true))
			job.setOutputFormat(TextOutputFormat.class);
		else
			job.setOutputFormat(NullOutputFormat.class);

		TextOutputFormat.setOutputPath(job, outputPath);

		if (!params.getBoolean("background", false)) {
			LOG.info("Submit job in sync mode");
			RunningJob runningJob = JobClient.runJob(job);
			Counters counters = runningJob.getCounters();
			Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
			final long resultCount = outputRecordCounter.getValue();

			// Output number of running map tasks
			Counter mapTaskCountCounter = counters.findCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
			System.out.println("Number of map tasks " + mapTaskCountCounter.getValue());

			// Delete output directory if not explicitly set by user
			if (userOutputPath == null)
				fs[0].delete(outputPath, true);
			long t2 = System.currentTimeMillis();
			System.out.println("Join time " + (t2 - t1) + " millis");

			return resultCount;
		} else {
			JobClient jc = new JobClient(job);
			LOG.info("Submit job in async mode");
			lastRunningJob = jc.submitJob(job);
			LOG.info("Job " + lastRunningJob + " submitted successfully");
			return -1;
		}
	}

	private static void printUsage() {
		System.out.println("Performs a spatial join between two files using the distributed join algorithm");
		System.out.println("Parameters: (* marks the required parameters)");
		System.out.println("<input file 1> - (*) Path to the first input file");
		System.out.println("<input file 2> - (*) Path to the second input file");
		System.out.println("<output file> - Path to output file");
		System.out.println("-overwrite - Overwrite output file without notice");

		GenericOptionsParser.printGenericCommandUsage(System.out);
	}


	private static DistanceAndPairGeneric<Shape> calculateSampleThreshold2(FileSystem[] fs, Path[] inFiles, long t1, JobConf job) throws IOException, InterruptedException {
		GlobalIndex<Partition> gIndex1 = SpatialSite.getGlobalIndex(fs[0], inFiles[0]);
		GlobalIndex<Partition> gIndex2 = SpatialSite.getGlobalIndex(fs[1], inFiles[1]);

		float sample_ratio = job.getFloat("ratio", 0.01f);
		float alpha = job.getFloat("alpha", 0.0f);


		LOG.info("Sample ratio "+sample_ratio+" "+job.getLong("seed", System.currentTimeMillis()));

		final Vector<Point> sample = new Vector<Point>();
		final Vector<Point> sample2 = new Vector<Point>();

		Partition r = null;
		Partition s = null;
		double maxDensity = 0;
		long auxRelements = 0;
		long auxSelements = 0;

		for (Partition auxR : gIndex1) {
			auxRelements += auxR.recordCount;
			auxSelements = 0;
			for (Partition auxS : gIndex2) {
				auxSelements += auxS.recordCount;
				Partition auxP = auxR.clone();
				auxP.expand(auxS);
				double density = auxP.recordCount/(auxP.getHeight()*auxP.getWidth());
				Rectangle auxIntersection = auxR.getIntersection(auxS);
				double intersectionArea = auxIntersection == null? 0:auxIntersection.getHeight()*auxIntersection.getWidth();
				double densityFinal = density*(1+intersectionArea);
				if(densityFinal>maxDensity){
					maxDensity = densityFinal;
					r=auxR.clone();
					s=auxS.clone();
				}
			}
		}
		long sample_count_r = (long) (auxRelements * sample_ratio);
		float sample_ratio_r = 1.0f;
		if(r.recordCount>sample_count_r)
			sample_ratio_r = ((float)sample_count_r)/((float)r.recordCount);

		long sample_count_s = (long) (auxSelements * sample_ratio);
		float sample_ratio_s = 1.0f;
		if(s.recordCount>sample_count_s)
			sample_ratio_s = ((float)sample_count_s)/((float)s.recordCount);



        Path partitionPath = new Path(inFiles[0], r.filename);
        long length = fs[0].getFileStatus(partitionPath).getLen();
        FileSplit fsplit = new FileSplit(partitionPath, 0, length, new String[0]);
		ShapeLineInputFormat inputFormat = new ShapeLineInputFormat();
		RecordReader<Rectangle, Text> reader = inputFormat.getRecordReader(fsplit, job, null);

		int numberk = job.getInt("k", 1);


		Rectangle key = reader.createKey();
		Text value = reader.createValue();

		Point point = new Point();

		if(alpha != 0.0f){

			long record_count = 0;

			while (reader.next(key, value) && record_count < maxShapesInOneRead) {
				point.fromText(value);
				sample.add(point.clone());
				record_count++;
			}

		}else {

			Random random = new Random(job.getLong("seed", System.currentTimeMillis()));

			long record_count = 0;

			while (reader.next(key, value)) {
				if (random.nextFloat() < sample_ratio_r || record_count < numberk) {
					point.fromText(value);
					sample.add(point.clone());
					record_count++;

				}
			}
		}

		reader.close();


		partitionPath = new Path(inFiles[1], s.filename);
        length = fs[1].getFileStatus(partitionPath).getLen();
        fsplit = new FileSplit(partitionPath, 0, length, new String[0]);

		inputFormat = new ShapeLineInputFormat();
		reader = inputFormat.getRecordReader(fsplit, job, null);


		if(alpha != 0.0f){

			long record_count = 0;

			while (reader.next(key, value) && record_count < maxShapesInOneRead) {
				point.fromText(value);
				sample2.add(point.clone());
				record_count++;
			}

		}else {

			long record_count = 0;

			Random random = new Random(job.getLong("seed", System.currentTimeMillis()));

			key = reader.createKey();
			value = reader.createValue();

			while (reader.next(key, value)) {
				if (random.nextFloat() < sample_ratio_s || record_count < numberk) {
					point.fromText(value);
					sample2.add(point.clone());
					record_count++;
				}
			}
		}


		reader.close();



		Shape[] p = new Shape[sample.size()];
		sample.toArray(p);
		Shape[] q = new Shape[sample2.size()];
		sample2.toArray(q);

		Arrays.sort(p);

		Arrays.sort(q);

		PriorityQueue<DistanceAndPairGeneric<Shape>> queue = null;
		int algorithm = job.getInt("alg", 1);
		switch (algorithm) {
			case 4:
			case 5:
			case 6:
				queue = reverseKCPQuery(p, q, numberk, algorithm, null, alpha, null);
				break;
			default:
				break;
		}
		DistanceAndPairGeneric<Shape> beta = queue.pop();
		LOG.info("beta " + beta);
		return beta;
	}


	private static DistanceAndPairGeneric<Shape> calculateSampleThreshold(Path[] inFiles, long t1, JobConf job) throws IOException {
		final Vector<Point> sample = new Vector<Point>();
		float sample_ratio = job.getFloat("ratio", 0.001f);
		long sample_size = job.getLong("size", 100 * 1024 * 1024);

		LOG.info("Reading a sample of " + Math.round(sample_ratio * 100) + "%");
		ResultCollector<Point> resultCollector = new ResultCollector<Point>() {
			@Override
			public void collect(Point p) {
				sample.add(p.clone());
			}
		};

		final Vector<Point> sample2 = new Vector<Point>();

		LOG.info("Reading a sample of " + Math.round(sample_ratio * 100) + "%");
		ResultCollector<Point> resultCollector2 = new ResultCollector<Point>() {
			@Override
			public void collect(Point p) {
				sample2.add(p.clone());
			}
		};

		OperationsParams params2 = new OperationsParams();
		params2.setFloat("ratio", sample_ratio);
		params2.setLong("size", sample_size);
		if (job.get("shape") != null)
			params2.set("shape", job.get("shape"));
		if (job.get("local") != null)
			params2.set("local", job.get("local"));
		params2.setClass("outshape", Point.class, Shape.class);
		Path[] inFiles0 = { inFiles[0] };

		Sampler.sample(inFiles0, resultCollector, params2);
		long t_2 = System.currentTimeMillis();
		System.out.println("Total time for sampling in millis: " + (t_2 - t1));
		LOG.info("Finished reading a sample of " + sample.size() + " records");

		params2.setFloat("ratio", sample_ratio);
		params2.setLong("size", sample_size);
		if (job.get("shape") != null)
			params2.set("shape", job.get("shape"));
		if (job.get("local") != null)
			params2.set("local", job.get("local"));
		params2.setClass("outshape", Point.class, Shape.class);
		Path[] inFiles1 = { inFiles[1] };

		Sampler.sample(inFiles1, resultCollector2, params2);
		long t_3 = System.currentTimeMillis();
		System.out.println("Total time for sampling in millis: " + (t_3 - t_2));
		LOG.info("Finished reading a sample of " + sample2.size() + " records");

		Shape[] p = new Shape[sample.size()];
		sample.toArray(p);
		Shape[] q = new Shape[sample2.size()];
		sample2.toArray(q);

		Arrays.sort(p);

		Arrays.sort(q);

		int numberk = job.getInt("k", 1);

		LOG.info("Getting beta for k = "+numberk);
		PriorityQueue<DistanceAndPairGeneric<Shape>> queue = null;
		int algorithm = job.getInt("alg", 1);
		switch (algorithm) {
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
			case 6:
				queue = reverseKCPQuery(p, q, numberk, algorithm, null);
				break;
			default:
				break;
		}
		DistanceAndPairGeneric<Shape> beta = queue.pop();
		LOG.info("beta " + beta);
		return beta;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] allFiles = params.getPaths();
		if (allFiles.length < 2) {
			System.err.println("This operation requires at least two input files");
			printUsage();
			System.exit(1);
		}
		if (allFiles.length == 2 && !params.checkInput()) {
			// One of the input files does not exist
			printUsage();
			System.exit(1);
		}
		if (allFiles.length > 2 && !params.checkInputOutput()) {
			printUsage();
			System.exit(1);
		}

		Path[] inputPaths = allFiles.length == 2 ? allFiles : params.getInputPaths();
		Path outputPath = allFiles.length == 2 ? null : params.getOutputPath();

		long result_size;
		result_size = kcpqStep(inputPaths, outputPath, params);

		System.out.println("Result size: " + result_size);
	}
}
