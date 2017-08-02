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
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * An implementation of Spatial Join MapReduce as described in S. Zhang, J. Han,
 * Z. Liu, K. Wang, and Z. Xu. KNNJ: Parallelizing spatial join with MapReduce
 * on clusters. In CLUSTER, pages 1–8, New Orleans, LA, Aug. 2009. The map
 * function partitions data into grid cells and the reduce function makes a
 * plane-sweep over each cell.
 *
 * @author eldawy
 */
public class KNNJ {

    /**
     * Class logger
     */
    private static final Log LOG = LogFactory.getLog(KNNJ.class);
    private static final String PartitionGrid = "KNNJ.PartitionGrid";
    public static final String PartitioiningFactor = "partition-grid-factor";
    private static final String InactiveMode = "KNNJ.InactiveMode";
    private static final String isFilterOnlyMode = "DJ.FilterOnlyMode";
    private static final String JoiningThresholdPerOnce = "DJ.JoiningThresholdPerOnce";
    public static boolean isReduceInactive = false;
    public static boolean isSpatialJoinOutputRequired = true;
    public static boolean isFilterOnly = false;
    public static int joiningThresholdPerOnce = 50000;


    //Divide el espacio en base a las celdas del segund conjunto


    public static class IndexedText implements Writable {
        public byte index;
        public Text text;

        IndexedText() {
            text = new Text();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(index);
            text.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            index = in.readByte();
            text.readFields(in);
        }
    }

    public static class ShapeWithDistance implements Writable, Cloneable {

        public Shape shape;
        public double distance;

        @Override
        public void write(DataOutput out) throws IOException {
            shape.write(out);
            out.writeDouble(distance);

        }

        @Override
        public void readFields(DataInput in) throws IOException {
            shape.readFields(in);
            distance = in.readDouble();

        }

    }

    /**
     * Separa los archivos de entrada en 2 listas asociandoles la cell id adecuada
     *
     * @author Ahmed Eldawy
     */
    public static class KNNJMap2 extends MapReduceBase implements Mapper<Rectangle, Text, IntWritable, IndexedText> {
        private Shape shape;
        private IndexedText outputValue = new IndexedText();
        private PartitionInfo gridInfo;
        private IntWritable cellId = new IntWritable();
        private Path[] inputFiles;
        private InputSplit currentSplit;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            // Retrieve grid to use for partitioning
            gridInfo = (PartitionInfo) OperationsParams.getShape(job, PartitionGrid);
            // Create a stock shape for deserializing lines
            shape = SpatialSite.createStockShape(job);
            // Get input paths to determine file index for every record
            inputFiles = FileInputFormat.getInputPaths(job);
        }

        @Override
        public void map(Rectangle cellMbr, Text value, OutputCollector<IntWritable, IndexedText> output,
                        Reporter reporter) throws IOException {
            if (reporter.getInputSplit() != currentSplit) {
                FileSplit fsplit = (FileSplit) reporter.getInputSplit();
                for (int i = 0; i < inputFiles.length; i++) {
                    if (fsplit.getPath().toString().startsWith(inputFiles[i].toString())) {
                        outputValue.index = (byte) i;
                    }
                }
                currentSplit = reporter.getInputSplit();
            }

            Text tempText = new Text(value);
            outputValue.text = value;
            shape.fromText(tempText);
            Rectangle shape_mbr = shape.getMBR();
            // Do a reference point technique to avoid processing the same
            // record twice
            if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {
                Rectangle shapeMBR = shape.getMBR();
                if (shapeMBR == null)
                    return;

                List<Partition> cells = gridInfo.getOverlappingCells(shapeMBR);
                for (Partition part : cells) {
                    cellId.set(part.cellId);
                    output.collect(cellId, outputValue);
                }
            }
        }
    }

    // Recrea las listas de puntos R y S y calcula KNNJ (RxS)
    public static class KNNJReduce2<S extends Shape> extends MapReduceBase
            implements Reducer<IntWritable, IndexedText, S, ArrayWritable> {
        /**
         * Class logger
         */
        private static final Log KNNJReduceLOG = LogFactory.getLog(KNNJReduce2.class);

        /**
         * Number of files in the input
         */
        private int inputFileCount;

        /**
         * List of cells used by the reducer
         */
        private int shapesThresholdPerOnce;

        private S shape;
        private int k;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            shape = (S) SpatialSite.createStockShape(job);
            shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
            inputFileCount = FileInputFormat.getInputPaths(job).length;
            k = job.getInt("k", 1);
            KNNJReduceLOG.info("configured the reduced task");
        }

        @Override
        public void reduce(IntWritable cellId, Iterator<IndexedText> values,
                           final OutputCollector<S, ArrayWritable> output, Reporter reporter) throws IOException {

            LOG.info("Start reduce() logic now !!!");
            long t1 = System.currentTimeMillis();


            // Partition retrieved shapes (values) into lists for each file
            List<S>[] shapeLists = new List[inputFileCount];
            for (int i = 0; i < shapeLists.length; i++) {
                shapeLists[i] = new Vector<S>();
            }

            while (values.hasNext()) {
                do {
                    IndexedText t = values.next();
                    S s = (S) shape.clone();
                    s.fromText(t.text);
                    shapeLists[t.index].add(s);
                } while (values.hasNext() /*
											 * && shapeLists[1].size() <
											 * shapesThresholdPerOnce
											 */);

                // Perform spatial join between the two lists
                KNNJReduceLOG.info("Joining (" + shapeLists[0].size() + " X " + shapeLists[1].size() + ")...");

                SpatialAlgorithms.KNNJoin_planeSweep(shapeLists[0], shapeLists[1], cellId.get(), k,
                        new ResultCollector2<S, ArrayWritable>() {

                            @Override
                            public void collect(S x, ArrayWritable y) {
                                try {

                                    output.collect(x, y);

                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }, reporter);

                shapeLists[1].clear();
            }

            long t2 = System.currentTimeMillis();
            LOG.info("Reducer finished in: " + (t2 - t1) + " millis");

        }
    }

    public static class PartitionInfo extends Rectangle {


        /**All underlying shapes in no specific order*/
        protected List<Partition> shapes;

        public PartitionInfo(){
            super();
            shapes = new ArrayList<Partition>();
        }

        public PartitionInfo(double x1, double y1, double x2, double y2) {
            super(x1, y1, x2, y2);
            shapes = new ArrayList<Partition>();
        }


        @Override
        public Text toText(Text text) {
            final byte[] Comma = ",".getBytes();
            final byte[] Null = "\0".getBytes();
            super.toText(text);
            text.append(Comma, 0, Comma.length);
            TextSerializerHelper.serializeInt(shapes.size(),text,',');
            boolean first = true;
            for(Partition part : shapes){
                if(first){
                    first = false;
                }else{
                    text.append(Comma, 0, Comma.length);
                }
                part.toText(text);
            }
            text.append(Null, 0, Null.length);
            return text;
        }

        @Override
        public void fromText(Text text) {
            super.fromText(text);
            if (text.getLength() > 0) {
                // Remove the first comma
                text.set(text.getBytes(), 1, text.getLength() - 1);
                int shapesLength = TextSerializerHelper.consumeInt(text,',');
                shapes = new ArrayList<Partition>();
                Partition partAux = new Partition();
                for(int n = 0; n < shapesLength; n++){
                    partAux.fromText(text);
                    shapes.add(partAux.clone());
                }
                text.set(text.getBytes(), 1, text.getLength() - 1);
            }
        }

        public List<Partition> getOverlappingCells(Shape shapeMBR) {
            List<Partition> overlapped = new ArrayList<Partition>();
            for(Partition partAux : shapes){
                if(partAux.isIntersected(shapeMBR))
                    overlapped.add(partAux.clone());
            }
            return overlapped;
        }

        public double getAverageCellWidth() {
            return shapes.size()>0 ? x2-x1 / shapes.size() : 0;
        }
    }

    /**
     * The map class maps each object to all cells it overlaps with.
     *
     * @author Ahmed Eldawy
     */
    public static class KNNJMap3<S extends Shape> extends MapReduceBase implements Mapper<Rectangle, Text, IntWritable, IndexedText> {
        private S shape;
        private IndexedText outputValue = new IndexedText();
        private PartitionInfo gridInfo;
        private IntWritable cellId = new IntWritable();
        private Path[] inputFiles;
        private InputSplit currentSplit;
        private int k;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            // Retrieve grid to use for partitioning
            gridInfo = (PartitionInfo) OperationsParams.getShape(job, PartitionGrid);
            // Create a stock shape for deserializing lines
            shape = (S) SpatialSite.createStockShape(job);
            // Get input paths to determine file index for every record
            inputFiles = FileInputFormat.getInputPaths(job);
            k = job.getInt("k", 1);

        }

        @Override
        public void map(Rectangle cellMbr, Text value, OutputCollector<IntWritable, IndexedText> output,
                        Reporter reporter) throws IOException {
            if (reporter.getInputSplit() != currentSplit) {
                FileSplit fsplit = (FileSplit) reporter.getInputSplit();
                for (int i = 0; i < inputFiles.length; i++) {
                    if (fsplit.getPath().toString().startsWith(inputFiles[i].toString())) {
                        outputValue.index = (byte) i;
                    }
                }
                currentSplit = reporter.getInputSplit();
            }

            if (outputValue.index == 0) {
                Text tempText = new Text(value);
                Text tempShapeText = new Text();
                outputValue.text = value;
                shape.fromText(tempText);

                shape.toText(tempShapeText);
                outputValue.text = tempShapeText;

                Rectangle shape_mbr = shape.getMBR();

                //Remove tab
                tempText.set(tempText.getBytes(), 1, tempText.getLength() - 1);

                SpatialAlgorithms.TOPKNN<S> topKNN = new SpatialAlgorithms.TOPKNN<S>(shape,k);
                topKNN.fromText(tempText);

                //LOG.info("topKNN "+topKNN.cellId+"-"+topKNN.k+"-"+topKNN.heap );
                //if(true) throw new IOException("weeee");


                int auxCellId = topKNN.cellId;

                // Do a reference point technique to avoid processing the same
                // record twice
                if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {
                    Rectangle shapeMBR = shape.getMBR();
                    if (shapeMBR == null)
                        return;

                    double radius;
                    double radiusAux = 0;

                    if(topKNN.heap.size()>0){
                        radius = topKNN.heap.top().dist;
                    }else{
                        radius = gridInfo.getAverageCellWidth();
                    }

                    int elementCount = 0;

                    //LOG.info("radius "+radius );
                    //LOG.info("size "+ topKNN.heap.size() );


                    while (topKNN.heap.size() + elementCount < k) {
                        Rectangle shapeMBR2 = shape.getMBR().clone();
                        radiusAux += radius;
                        shapeMBR2.x1 -= radiusAux;
                        shapeMBR2.y1 -= radiusAux;
                        shapeMBR2.x2 += radiusAux;
                        shapeMBR2.y2 += radiusAux;
                        elementCount = 0;

                        List<Partition> cells = gridInfo.getOverlappingCells(shapeMBR);
                        for (Partition part : cells) {
                            int overlappingCellId = part.cellId;
                            if (overlappingCellId == auxCellId)
                                continue;
                            elementCount += part.recordCount;
                        }

                        if(shapeMBR2.contains(gridInfo)){
                            break;
                        }
                    }

                    if(radiusAux==0)
                        radiusAux = radius;


                    shapeMBR.x1 -= radiusAux;
                    shapeMBR.y1 -= radiusAux;
                    shapeMBR.x2 += radiusAux;
                    shapeMBR.y2 += radiusAux;

                    List<Partition> cells = gridInfo.getOverlappingCells(shapeMBR);
                    for (Partition part : cells) {
                        int overlappingCellId = part.cellId;
                        if (overlappingCellId == auxCellId)
                            continue;
                        cellId.set(overlappingCellId);
                        output.collect(cellId, outputValue);
                    }


                    //cellId.set(auxCellId);
                    //output.collect(cellId, outputValue);
                }
            } else {

                Text tempText = new Text(value);
                outputValue.text = value;
                shape.fromText(tempText);
                Rectangle shape_mbr = shape.getMBR();
                // Do a reference point technique to avoid processing the same
                // record twice
                if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {
                    Rectangle shapeMBR = shape.getMBR();
                    if (shapeMBR == null)
                        return;

                    List<Partition> cells = gridInfo.getOverlappingCells(shapeMBR);
                    for (Partition part : cells) {
                        cellId.set(part.cellId);
                        output.collect(cellId, outputValue);
                    }

                }
            }
        }
    }

    public static class KNNJReduce3<S extends Shape> extends MapReduceBase
            implements Reducer<IntWritable, IndexedText, S, ArrayWritable> {
        /**
         * Class logger
         */
        private static final Log KNNJReduceLOG = LogFactory.getLog(KNNJReduce2.class);

        /**
         * Number of files in the input
         */
        private int inputFileCount;

        /**
         * List of cells used by the reducer
         */
        private int shapesThresholdPerOnce;

        private S shape;
        private int k;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            shape = (S) SpatialSite.createStockShape(job);
            shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
            inputFileCount = FileInputFormat.getInputPaths(job).length;
            k = job.getInt("k", 1);
            KNNJReduceLOG.info("configured the reduced task");
        }

        @Override
        public void reduce(IntWritable cellId, Iterator<IndexedText> values,
                           final OutputCollector<S, ArrayWritable> output, Reporter reporter) throws IOException {

            LOG.info("Start reduce() logic now !!!");
            long t1 = System.currentTimeMillis();


            // Partition retrieved shapes (values) into lists for each file
            List<S>[] shapeLists = new List[inputFileCount];
            for (int i = 0; i < shapeLists.length; i++) {
                shapeLists[i] = new Vector<S>();
            }

            while (values.hasNext()) {
                do {
                    IndexedText t = values.next();
                    S s = (S) shape.clone();
                    s.fromText(t.text);
                    shapeLists[t.index].add(s);
                } while (values.hasNext() /*
									 * && shapeLists[1].size() <
									 * shapesThresholdPerOnce
									 */);

                // Perform spatial join between the two lists
                KNNJReduceLOG.info("Joining (" + shapeLists[0].size() + " X " + shapeLists[1].size() + ")...");

                SpatialAlgorithms.KNNJoin_planeSweep(shapeLists[0], shapeLists[1], cellId.get(), k,
                        new ResultCollector2<S, ArrayWritable>() {

                            @Override
                            public void collect(S x, ArrayWritable y) {
                                try {

                                    output.collect(x, y);

                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }, reporter);

                shapeLists[1].clear();
            }

            long t2 = System.currentTimeMillis();
            LOG.info("Reducer finished in: " + (t2 - t1) + " millis");

        }
    }

    /**
     * The map class maps each object to all cells it overlaps with.
     *
     * @author Ahmed Eldawy
     */
    public static class KNNJMap4 extends MapReduceBase implements Mapper<Rectangle, Text, Shape, SpatialAlgorithms.TOPKNN<Shape>> {
        private Shape shape;
        private int k;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            // Create a stock shape for deserializing lines
            shape = SpatialSite.createStockShape(job);
            k = job.getInt("k", 1);

        }

        @Override
        public void map(Rectangle cellMbr, Text value, OutputCollector<Shape, SpatialAlgorithms.TOPKNN<Shape>> output,
                        Reporter reporter) throws IOException {

            Text tempText = new Text(value);
            shape.fromText(tempText);

            //Remove tab
            tempText.set(tempText.getBytes(), 1, tempText.getLength() - 1);

            SpatialAlgorithms.TOPKNN<Shape> topKNN = new SpatialAlgorithms.TOPKNN<Shape>(shape,k);
            topKNN.fromText(tempText);
            output.collect(shape.clone(),topKNN);


        }
    }

    public static class KNNJReduce4 extends MapReduceBase
            implements Reducer<Shape, SpatialAlgorithms.TOPKNN<Shape>, Shape, SpatialAlgorithms.TOPKNN<Shape>> {
        /**
         * Class logger
         */
        private static final Log KNNJReduceLOG = LogFactory.getLog(KNNJReduce4.class);

        /**
         * Number of files in the input
         */
        private int inputFileCount;

        /**
         * List of cells used by the reducer
         */
        private GridInfo grid;
        private int shapesThresholdPerOnce;

        private Shape stockShape;
        private int k;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            grid = (GridInfo) OperationsParams.getShape(job, PartitionGrid);
            stockShape = SpatialSite.createStockShape(job);
            shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
            inputFileCount = FileInputFormat.getInputPaths(job).length;
            k = job.getInt("k", 1);
            KNNJReduceLOG.info("configured the reduced task");
        }

        @Override
        public void reduce(Shape shape, Iterator<SpatialAlgorithms.TOPKNN<Shape>> values,
                           final OutputCollector<Shape, SpatialAlgorithms.TOPKNN<Shape>> output, Reporter reporter) throws IOException {

            LOG.info("Start reduce() logic now !!!");
            long t1 = System.currentTimeMillis();
            LOG.info("FORMA "+shape);
            int num = 0;

            SpatialAlgorithms.TOPKNN<Shape> finalTopKNN = new SpatialAlgorithms.TOPKNN<Shape>(stockShape,k);
            while (values.hasNext()) {
                SpatialAlgorithms.TOPKNN<Shape> auxTopKNN = values.next();
                SpatialAlgorithms.ShapeNN<Shape> auxShape;
                num++;
                while( (auxShape = auxTopKNN.heap.pop()) != null){
                    finalTopKNN.add(auxShape.shape,auxShape.dist);
                }
            }

            LOG.info(num);

            output.collect(shape,finalTopKNN);

            long t2 = System.currentTimeMillis();
            LOG.info("Reducer finished in: " + (t2 - t1) + " millis");

        }
    }

    public static <S extends Shape> long KNNJ(Path[] inFiles, Path userOutputPath, OperationsParams params)
            throws IOException, InterruptedException {
        JobConf job = new JobConf(params, KNNJ.class);

        LOG.info("KNNJ journey starts ....");
        FileSystem inFs = inFiles[0].getFileSystem(job);
        Path outputPath = userOutputPath;
        if (outputPath == null) {
            FileSystem outFs = FileSystem.get(job);
            do {
                outputPath = new Path(inFiles[0].getName() + ".KNNJ_" + (int) (Math.random() * 1000000));
            } while (outFs.exists(outputPath));
        }
        FileSystem outFs = FileSystem.get(job);


        ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
        job.setJobName("KNNJ");
        job.setMapperClass(KNNJMap2.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IndexedText.class);
        job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));
        job.setLong("mapred.min.split.size",
                Math.max(inFs.getFileStatus(inFiles[0]).getBlockSize(), inFs.getFileStatus(inFiles[1]).getBlockSize()));

        job.setReducerClass(KNNJReduce2.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        if (job.getBoolean("output", true))
            job.setOutputFormat(TextOutputFormat.class);
        else
            job.setOutputFormat(NullOutputFormat.class);
        ShapeLineInputFormat.setInputPaths(job, inFiles);


        // Calculate and set the dimensions of the grid to use in the map phase
        long total_size = 0;
        Rectangle mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
        for (Path file : inFiles) {
            FileSystem fs = file.getFileSystem(params);
            Rectangle file_mbr = FileMBR.fileMBR(file, params);
            mbr.expand(file_mbr);
            total_size += FileUtil.getPathSize(fs, file);
        }
        // If the largest file is globally indexed, use its partitions
        total_size += total_size * job.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.2f);
        int KNNJPartitioningGridFactor = params.getInt(PartitioiningFactor, 20);
        int num_cells = (int) Math.max(1,
                total_size * KNNJPartitioningGridFactor / outFs.getDefaultBlockSize(outputPath));
        LOG.info("Number of cells is configured to be " + num_cells);

        OperationsParams.setInactiveModeFlag(job, InactiveMode, isReduceInactive);
        OperationsParams.setJoiningThresholdPerOnce(job, JoiningThresholdPerOnce, joiningThresholdPerOnce);
        OperationsParams.setFilterOnlyModeFlag(job, isFilterOnlyMode, isFilterOnly);


        TextOutputFormat.setOutputPath(job, outputPath);

        if (OperationsParams.isLocal(job, inFiles)) {
            // Enforce local execution if explicitly set by user or for small
            // files
            job.set("mapred.job.tracker", "local");
        }



        TextInputFormat inputFormat = new TextInputFormat();
        inputFormat.configure(job);
        TextInputFormat.setInputPaths(job, inFiles);

        job.setMapperClass(KNNJMap2.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IndexedText.class);

        job.setReducerClass(KNNJReduce2.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        ShapeLineInputFormat.setInputPaths(job, inFiles);

        do {
            outputPath = new Path(userOutputPath.getName() + ".KNNJ2_" + (int) (Math.random() * 1000000));
        } while (outFs.exists(outputPath));

        TextOutputFormat.setOutputPath(job, outputPath);

        RunningJob runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();

        outFs.deleteOnExit(outputPath);

        GlobalIndex<Partition> gIndex2 = SpatialSite.getGlobalIndex(inFiles[1].getFileSystem(job), inFiles[1]);
        Rectangle gIndex2MBR = gIndex2.getMBR();
        PartitionInfo partInfo = new PartitionInfo(gIndex2MBR.x1,gIndex2MBR.y1,gIndex2MBR.x2,gIndex2MBR.y2);
        for (Partition auxR : gIndex2) {

            partInfo.shapes.add(auxR.clone());

        }

        OperationsParams.setShape(job, PartitionGrid, partInfo);

        job.setMapperClass(KNNJMap3.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IndexedText.class);

        job.setReducerClass(KNNJReduce3.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        Path[] inputFiles3 =  new Path[2];
        inputFiles3[0] = outputPath;
        inputFiles3[1] = inFiles[1];
        ShapeLineInputFormat.setInputPaths(job, inputFiles3);

        do {
            outputPath = new Path(userOutputPath.getName() + ".KNNJ3_" + (int) (Math.random() * 1000000));
        } while (outFs.exists(outputPath));

        TextOutputFormat.setOutputPath(job, outputPath);

        runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();

        job.setMapperClass(KNNJMap4.class);

        Shape stockShape = params.getShape("shape");

        job.setMapOutputKeyClass(stockShape.getClass());
        job.setMapOutputValueClass(SpatialAlgorithms.TOPKNN.class);

        job.setReducerClass(KNNJReduce4.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        inputFiles3[1] = outputPath;

        //ShapeLineInputFormat.setInputPaths(job, inputFiles3);
        ShapeLineInputFormat.setInputPaths(job, inFiles);


        TextOutputFormat.setOutputPath(job, userOutputPath);

        runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();



        Counters counters = runningJob.getCounters();
        Counter outputRecordCounter = counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
        final long resultCount = outputRecordCounter.getValue();

        return resultCount;
    }

    private static void printUsage() {
        System.out.println("Performs a spatial join between two files using the distributed join algorithm");
        System.out.println("Parameters: (* marks the required parameters)");
        System.out.println("<input file 1> - (*) Path to the first input file");
        System.out.println("<input file 2> - (*) Path to the second input file");
        System.out.println("<output file> - Path to output file");
        System.out.println("partition-grid-factor:<value> - Patitioning grid factor (its default value is 20)");
        System.out.println("-overwrite - Overwrite output file without notice");
        GenericOptionsParser.printGenericCommandUsage(System.out);
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
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

        if (params.get("repartition-only", "no").equals("yes")) {
            isReduceInactive = true;
        }

        if (params.get("joining-per-once") != null) {
            System.out.println("joining-per-once is set to: " + params.get("joining-per-once"));
            joiningThresholdPerOnce = Integer.parseInt(params.get("joining-per-once"));
        }

        if (params.get("filter-only") != null) {
            System.out.println("filer-only mode is set to: " + params.get("filter-only"));
            if (params.get("filter-only").equals("yes")) {
                isFilterOnly = true;
            } else {
                isFilterOnly = false;
            }
        }

        if (params.get("no-output") != null) {
            System.out.println("no-output mode is set to: " + params.get("no-output"));
            if (params.get("no-output").equals("yes")) {
                isSpatialJoinOutputRequired = false;
            } else {
                isSpatialJoinOutputRequired = true;
            }
        }

        long t1 = System.currentTimeMillis();
        long resultSize = KNNJ(inputPaths, outputPath, params);
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: " + (t2 - t1) + " millis");
        System.out.println("Result size: " + resultSize);
    }

}
