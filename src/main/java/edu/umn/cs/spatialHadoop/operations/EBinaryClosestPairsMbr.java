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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


/**
 * Performs a spatial join between two or more files using the redistribute-join
 * algorithm.
 *
 * @author Ahmed Eldawy
 *
 */
public class EBinaryClosestPairsMbr {
	private static final Log LOG = LogFactory.getLog(EBinaryClosestPairsMbr.class);
	public static RunningJob lastRunningJob;
	public static int maxBytesInOneRead = 1024 * 1024 * 100;
	public static int maxShapesInOneRead = 2000;
	public static boolean isOneShotReadMode = true;

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

	private static Double calculateDistance(Rectangle r1, Rectangle r2, double gdmax, int algorithm, Reporter reporter){

		double dx, dy, ssdx, gdmax2;
		Double distance;
		int realAlgorithm = (algorithm-1) % 3;


		dx = 0;
		if (r2.x1 > r1.x2)
			dx = r2.x1 - r1.x2;
		else if (r1.x1 > r2.x2)
			dx = r1.x1 - r2.x2;


		if (dx > gdmax) {
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

		if (dy > gdmax) {
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
		if (ssdx < gdmax2) {
			if (reporter != null)
				reporter.getCounter(SpatialAlgorithms.KCPCounters.NUM_OPERATIONS).increment(1);
			distance = Math.sqrt(ssdx);
			return distance;
		}

		return Double.MAX_VALUE;

	}

	public static void ecpq_rcps(Shape[] p1, Shape[] q1, Double epsilon,final ResultCollector2<Shape, Shape> output,
			final Reporter reporter)
					throws IOException, InterruptedException {

		int i = 0, j = 0, k, stk; // local counter of the points
		double dx, dy, ssdx, gdmax = epsilon;
		Double distance;
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

						distance = calculateDistance(refo,curo,gdmax, 6, reporter);

						if(distance==null){
							leftq = k;
							break;
						}else if(distance<0){
							continue;
						}

						if ( distance < gdmax) {
							aux.distance = distance;
							aux.pair.second = curo.clone();
							aux.pair.first = refo.clone();
							output.collect(aux.pair.first, aux.pair.second);
						} // end of if(d==NUM_DIM)
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

						distance = calculateDistance(refo,curo,gdmax, 6, reporter);

						if(distance==null){
							leftp = k;
							break;
						}else if(distance<0){
							continue;
						}

						if ( distance < gdmax) {

							aux.distance = distance;
							aux.pair.second = curo.clone();
							aux.pair.first = refo.clone();
							output.collect(aux.pair.second, aux.pair.first);

						} // end of if(d==NUM_DIM)

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
	}

	public static class EpsilonJoinFilter extends DefaultBlockFilter {

		private double epsilon = -1;

		@Override
		public void configure(Configuration conf) {
			super.configure(conf);
			epsilon = Double.parseDouble(conf.get("epsilon", "-1"));

		}

		@Override
		public void selectCellPairs(GlobalIndex<Partition> gIndex1, GlobalIndex<Partition> gIndex2,
				final ResultCollector2<Partition, Partition> output) {

			// TODO PLANE SWEEP
			for (Partition r : gIndex1) {
				for (Partition s : gIndex2) {

					double minDistance = r.getMinDistance(s);
					LOG.info("minDistance > epsilon? " + minDistance + " : " + epsilon);
					if (epsilon != -1 && minDistance > epsilon) {
						LOG.info("Skipping partitions " + r + ", " + s);
					} else {

						output.collect(r, s);

					}
				}
			}
		}
	}

	public static class EpsilonJoinMap extends MapReduceBase
			implements Mapper<PairWritable<Rectangle>, PairWritable<? extends Writable>, Shape, Shape> {

		private double epsilon = -1;
		private int algorithm;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			epsilon = Double.parseDouble(job.get("epsilon", "-1"));
			algorithm = job.getInt("alg", 1);

		}

		public void map(final PairWritable<Rectangle> key, final PairWritable<? extends Writable> value,
				final OutputCollector<Shape, Shape> output, final Reporter reporter) throws IOException {

			final Rectangle mapperMBR = !key.first.isValid() && !key.second.isValid() ? null // Both
																								// blocks
																								// are
																								// heap
																								// blocks
					: (!key.first.isValid() ? key.second // Second block is
															// indexed
							: (!key.second.isValid() ? key.first // First block
																	// is
																	// indexed
									: (key.first.getIntersection(key.second)))); // Both
																					// indexed

			if (value.first instanceof RTree && value.second instanceof RTree) {
				// Join two R-trees
				@SuppressWarnings("unchecked")
				RTree<Shape> r1 = (RTree<Shape>) value.first;
				@SuppressWarnings("unchecked")
				RTree<Shape> r2 = (RTree<Shape>) value.second;
				LOG.info("DEPURANDO " + epsilon);
				int result = RTree.spatialEpsilonJoin(r1, r2, epsilon, new ResultCollector2<Shape, Shape>() {
					@Override
					public void collect(Shape r, Shape s) {
						try {
							output.collect(r, s);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				}, reporter);

				LOG.info("RESULTADO " + result);
			} else if (value.first instanceof ArrayWritable && value.second instanceof ArrayWritable) {
				Shape[] p = (Shape[]) ((ArrayWritable) value.first).get();
				Shape[] q = (Shape[]) ((ArrayWritable) value.second).get();

				/*
				 * if (p.length == 1) { // Cannot compute closest pair for an
				 * input of size 1 //out.collect(Dummy, (Point) a[0]); return; }
				 */
				LOG.info("ordenando p de longitud " + p.length);

				Arrays.sort(p);

				LOG.info("ordenando q de longitud " + q.length);
				Arrays.sort(q);

				ResultCollector2<Shape, Shape> collector = new ResultCollector2<Shape, Shape>() {
					@Override
					public void collect(Shape r, Shape s) {
						try {
							output.collect(r, s);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				};

				LOG.info("Calculando epsilon");
				try{
					switch (algorithm) {
					default:
						ecpq_rcps(p, q, epsilon, collector, reporter);
						break;
					}
				}catch(InterruptedException ie){
					throw new RuntimeException("Error doing algorithm "+algorithm);
				}

			} else {
				throw new RuntimeException(
						"Cannot join " + value.first.getClass() + " with " + value.second.getClass());
			}
			reporter.progress();
		}
	}

	/**
	 * Input format that returns a record reader that reads a pair of arrays of
	 * shapes
	 *
	 * @author Ahmed Eldawy
	 *
	 */
	public static class EpsilonJInputFormatArray extends BinarySpatialInputFormat<Rectangle, ArrayWritable> {

		/**
		 * Reads a pair of arrays of shapes
		 *
		 * @author Ahmed Eldawy
		 *
		 */
		public static class EpsilonJRecordReader extends BinaryRecordReader<Rectangle, ArrayWritable> {
			public EpsilonJRecordReader(Configuration conf, CombineFileSplit fileSplits) throws IOException {
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
			return new EpsilonJRecordReader(job, (CombineFileSplit) split);
		}
	}

	/**
	 * Input format that returns a record reader that reads a pair of arrays of
	 * shapes
	 *
	 * @author Ahmed Eldawy
	 *
	 */
	public static class EpsilonJInputFormatRTree<S extends Shape>
			extends BinarySpatialInputFormat<Rectangle, RTree<S>> {

		/**
		 * Reads a pair of arrays of shapes
		 *
		 * @author Ahmed Eldawy
		 *
		 */
		public static class EpsilonJRecordReader<S extends Shape> extends BinaryRecordReader<Rectangle, RTree<S>> {
			public EpsilonJRecordReader(Configuration conf, CombineFileSplit fileSplits) throws IOException {
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
			return new EpsilonJRecordReader<S>(job, (CombineFileSplit) split);
		}
	}

	public static <S extends Shape> long joinStep(Path[] inFiles, Path userOutputPath, OperationsParams params)
			throws IOException {
		long t1 = System.currentTimeMillis();

		JobConf job = new JobConf(params, EBinaryClosestPairsMbr.class);

		FileSystem fs[] = new FileSystem[inFiles.length];
		for (int i_file = 0; i_file < inFiles.length; i_file++)
			fs[i_file] = inFiles[i_file].getFileSystem(job);

		Path outputPath = userOutputPath;
		if (outputPath == null) {
			do {
				outputPath = new Path(inFiles[0].getName() + ".dj_" + (int) (Math.random() * 1000000));
			} while (fs[0].exists(outputPath));
		}

		job.setJobName("DistributedJoin");
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();

		LOG.info("Joining " + inFiles[0] + " X " + inFiles[1]);

		if (SpatialSite.isRTree(fs[0], inFiles[0]) && SpatialSite.isRTree(fs[1], inFiles[1])) {
			job.setInputFormat(EpsilonJInputFormatRTree.class);
		} else {
			if (isOneShotReadMode) {
				// Ensure all objects are read in one shot
				job.setInt(SpatialSite.MaxBytesInOneRead, -1);
				job.setInt(SpatialSite.MaxShapesInOneRead, -1);
			} else {
				job.setInt(SpatialSite.MaxBytesInOneRead, maxBytesInOneRead);
				job.setInt(SpatialSite.MaxShapesInOneRead, maxShapesInOneRead);
			}
			job.setInputFormat(EpsilonJInputFormatArray.class);
		}

		// Binary version of spatial join (two different input files)
		job.setClass(SpatialSite.FilterClass, EpsilonJoinFilter.class, BlockFilter.class);
		FileInputFormat.setInputPaths(job, inFiles);
		job.setMapperClass(EpsilonJoinMap.class);

		Shape shape = params.getShape("shape");
		job.setMapOutputKeyClass(shape.getClass());
		job.setMapOutputValueClass(shape.getClass());
		// job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
		job.setNumReduceTasks(0); // No reduce needed for this task

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
		result_size = joinStep(inputPaths, outputPath, params);

		System.out.println("Result size: " + result_size);
	}
}