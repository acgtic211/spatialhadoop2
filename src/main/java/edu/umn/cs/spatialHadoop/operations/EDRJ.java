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
import edu.umn.cs.spatialHadoop.indexing.QuadTreePartitioner;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;

/**
 * An implementation of Spatial Join MapReduce as described in S. Zhang, J. Han,
 * Z. Liu, epsilon. Wang, and Z. Xu. EDRJ: Parallelizing spatial join with MapReduce
 * on clusters. In CLUSTER, pages 1–8, New Orleans, LA, Aug. 2009. The map
 * function partitions data into grid cells and the reduce function makes a
 * plane-sweep over each cell.
 *
 * @author eldawy
 */
public class EDRJ {

    /**
     * Class logger
     */
    private static final Log LOG = LogFactory.getLog(EDRJ.class);
    private static final String PartitionGrid = "EDRJ.PartitionGrid";
    public static final String PartitioiningFactor = "partition-grid-factor";
    private static final String InactiveMode = "EDRJ.InactiveMode";
    private static final String isFilterOnlyMode = "DJ.FilterOnlyMode";
    private static final String JoiningThresholdPerOnce = "DJ.JoiningThresholdPerOnce";
    public static boolean isReduceInactive = false;
    public static boolean isSpatialJoinOutputRequired = true;
    public static boolean isFilterOnly = false;
    public static int joiningThresholdPerOnce = 50000;
    private static int SUBCELL_ID = 1000000000;

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
    public static class EDRJMap1 extends MapReduceBase implements Mapper<Rectangle, Text, Partition, Text> {
        private Shape shape;
        private Text outputValue = new Text();
        private PartitionInfo gridInfo;
        Random rand;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            // Retrieve grid to use for partitioning
            gridInfo = (PartitionInfo) OperationsParams.getShape(job, PartitionGrid);
            // Create a stock shape for deserializing lines
            shape = SpatialSite.createStockShape(job);
            // Get input paths to determine file index for every record

            rand = new Random();

        }

        @Override
        public void map(Rectangle cellMbr, Text value, OutputCollector<Partition, Text> output,
                        Reporter reporter) throws IOException {

            if(rand.nextFloat()>0.02){
                return;
            }

            Text tempText = new Text(value);
            outputValue = value;
            shape.fromText(tempText);
            Rectangle shape_mbr = shape.getMBR();
            // Do a reference point technique to avoid processing the same
            // record twice
            if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {

                List<Partition> cells = gridInfo.getOverlappingCells(shape_mbr);

                for (Partition part : cells) {

                    output.collect(part, outputValue);
                }
            }
        }
    }

    // Recrea las listas de puntos R y S y calcula EDRJ (RxS)
    public static class EDRJReduce1<S extends Shape> extends MapReduceBase
        implements Reducer<Partition, Text, IntWritable, QuadTreePartitioner> {
        /**
         * Class logger
         */
        private static final Log EDRJReduceLOG = LogFactory.getLog(EDRJReduce1.class);

        /**
         * Number of files in the input
         */
        private int inputFileCount;

        /**
         * List of cells used by the reducer
         */
        private int shapesThresholdPerOnce;

        private S shape;
        IntWritable aux;


        @Override
        public void configure(JobConf job) {
            super.configure(job);
            shape = (S) SpatialSite.createStockShape(job);
            shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
            inputFileCount = FileInputFormat.getInputPaths(job).length;
            EDRJReduceLOG.info("configured the reduced task");
            aux = new IntWritable();

        }

        @Override
        public void reduce(Partition cellId, Iterator<Text> values,
                           final OutputCollector<IntWritable, QuadTreePartitioner> output, Reporter reporter) throws IOException {

            LOG.info("Start reduce() logic now !!!");
            long t1 = System.currentTimeMillis();

            // Partition retrieved shapes (values) into lists for each file
            Vector<S> shapes  = new Vector<S>();


            while (values.hasNext()) {

                Text t = values.next();
                S s = (S) shape.clone();
                s.fromText(t);
                shapes.add(s);

            }
            Point finalShapes[] = new Point[shapes.size()];
            shapes.toArray(finalShapes);
            QuadTreePartitioner qt = new QuadTreePartitioner();
            qt.createFromPoints(cellId,finalShapes,(int)Math.floor(shapesThresholdPerOnce*0.02));

            aux.set(cellId.cellId);
            output.collect(aux,qt);


            long t2 = System.currentTimeMillis();
            LOG.info("Reducer finished in: " + (t2 - t1) + " millis");

        }

        @Override
        public void close() throws IOException {
            super.close();
        }
    }


    /**
     * Separa los archivos de entrada en 2 listas asociandoles la cell id adecuada
     *
     * @author Ahmed Eldawy
     */
    public static class EDRJMap2 extends MapReduceBase implements Mapper<Rectangle, Text, LongWritable, IndexedText> {
        private Shape shape;
        private IndexedText outputValue = new IndexedText();
        private PartitionInfo gridInfo;
        private LongWritable cellId = new LongWritable();
        private Path[] inputFiles;
        private InputSplit currentSplit;
        private Map<String, QuadTreePartitioner> qtrees;
        private Double epsilon;


        @Override
        public void configure(JobConf job) {
            super.configure(job);
            // Retrieve grid to use for partitioning
            gridInfo = (PartitionInfo) OperationsParams.getShape(job, PartitionGrid);
            // Create a stock shape for deserializing lines
            shape = SpatialSite.createStockShape(job);
            // Get input paths to determine file index for every record
            inputFiles = FileInputFormat.getInputPaths(job);
            epsilon = Double.parseDouble(job.get("epsilon", "-1"));


            String sizesAux = job.get("QTREES", null);
            if(sizesAux!=null){
                Map<String,String> aux = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),aux);
                qtrees = new HashMap<String, QuadTreePartitioner>();
                for(String key : aux.keySet()) {
                    QuadTreePartitioner qp = new QuadTreePartitioner();
                    qp.fromText(new Text(aux.get(key).getBytes()));
                    qtrees.put(key,qp);
                }
            }
        }

        @Override
        public void map(Rectangle cellMbr, Text value, final OutputCollector<LongWritable, IndexedText> output,
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

                Rectangle shapeMBR2 = shape.getMBR().clone();
                shapeMBR2.x1 -= epsilon;
                shapeMBR2.y1 -= epsilon;
                shapeMBR2.x2 += epsilon;
                shapeMBR2.y2 += epsilon;

                List<Partition> cells = gridInfo.getOverlappingCells(shape_mbr);

                for (final Partition part : cells) {

                    QuadTreePartitioner qp = qtrees.get(part.cellId+"");
                    if(qp==null){
                        output.collect(cellId, outputValue);
                        continue;
                    }

                    qp.overlapPartitions(shape_mbr,new ResultCollector<Integer>() {

                        @Override
                        public void collect(Integer x) {
                            long finalCellId = ((long)(part.cellId) * SUBCELL_ID) + x.longValue();
                            cellId.set(finalCellId);

                            try {
                                output.collect(cellId, outputValue);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });

                }

            }
        }
    }

    // Recrea las listas de puntos R y S y calcula EDRJ (RxS)
    public static class EDRJReduce2<S extends Shape> extends MapReduceBase
            implements Reducer<LongWritable, IndexedText, S, S> {
        /**
         * Class logger
         */
        private static final Log EDRJReduceLOG = LogFactory.getLog(EDRJReduce2.class);

        /**
         * Number of files in the input
         */
        private int inputFileCount;

        /**
         * List of cells used by the reducer
         */
        private int shapesThresholdPerOnce;

        private S shape;
        private Double epsilon;
        LongWritable aux;
        MultipleOutputs mos;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            shape = (S) SpatialSite.createStockShape(job);
            shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
            inputFileCount = FileInputFormat.getInputPaths(job).length;
            epsilon = Double.parseDouble(job.get("epsilon", "-1"));
            EDRJReduceLOG.info("configured the reduced task");
            aux = new LongWritable();
            mos = new MultipleOutputs(job);

        }

        @Override
        public void reduce(LongWritable cellId, Iterator<IndexedText> values,
                           final OutputCollector<S, S> output, Reporter reporter) throws IOException {

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
                EDRJReduceLOG.info("Joining (" + shapeLists[0].size() + " X " + shapeLists[1].size() + ")...");

                SpatialAlgorithms.EDRJoin_planeSweep(shapeLists[0], shapeLists[1], epsilon,
                        new ResultCollector2<S, S>() {

                            @Override
                            public void collect(S x, S y) {
                                try {

                                    output.collect(x, y);

                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }, reporter);

                //shapeLists[1].clear();
            }

            aux.set(shapeLists[1].size());
            mos.getCollector("test", reporter).collect(cellId, this.aux);

            long t2 = System.currentTimeMillis();
            LOG.info("Reducer finished in: " + (t2 - t1) + " millis");

        }

        @Override
        public void close() throws IOException {
            mos.close();
            super.close();
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
            //final byte[] Null = "\0".getBytes();
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
            //text.append(Null, 0, Null.length);
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

                if(text.getLength()>0)
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

        public Partition getNearestCell(Rectangle shapeMBR, int limit) {
            double minDistance = Double.MAX_VALUE;
            Partition thePart = null;
            for(Partition partAux : shapes){
                if(partAux.getMinDistance(shapeMBR)<minDistance) {
                    thePart = partAux;
                    minDistance = partAux.getMinDistance(shapeMBR);
                }
            }

            GridInfo giAux = new GridInfo(thePart.x1,thePart.y1,thePart.x2,thePart.y2);
            double factor = Math.sqrt((double)thePart.recordCount/(double)limit);
            int columns = Math.max(1,(int)Math.floor(factor));
            giAux.columns = columns;
            giAux.rows = columns;

            CellInfo[] celdas = giAux.getAllCells();
            minDistance = Double.MAX_VALUE;
            Partition aux = new Partition();
            for(CellInfo celda: celdas){
                if(celda.getMinDistance(shapeMBR)<minDistance) {
                    aux.cellId = (thePart.cellId) * SUBCELL_ID + celda.cellId;
                    minDistance = celda.getMinDistance(shapeMBR);
                }
            }
            return aux;
        }


        public Partition getPartition(int cellId) {
            for(Partition partAux : shapes){
                if(partAux.cellId == cellId){
                    return partAux;
                }
            }
            return null;
        }

        public Partition getNearestCell(Rectangle shapeMBR) {
            double minDistance = Double.MAX_VALUE;
            Partition thePart = null;
            for(Partition partAux : shapes){
                if(partAux.getMinDistance(shapeMBR)<minDistance) {
                    thePart = partAux;
                    minDistance = partAux.getMinDistance(shapeMBR);
                }
            }

            return thePart;

        }

        public List<Partition> getOverlappingSubCells(Rectangle shapeMBR, int limit) {
            List<Partition> overlapped = new ArrayList<Partition>();
            for(Partition partAux : shapes){
                if(partAux.isIntersected(shapeMBR))
                    overlapped.add(partAux.clone());
            }
            if(limit>0){
                List<Partition> finalOverlapped = new ArrayList<Partition>();

                for(Partition partAux : overlapped){
                    GridInfo giAux = new GridInfo(partAux.x1,partAux.y1,partAux.x2,partAux.y2);

                    double factor = Math.sqrt((double)partAux.recordCount/(double)limit);
                    int columns = Math.max(1,(int)Math.floor(factor));
                    giAux.columns = columns;
                    giAux.rows = columns;


                    java.awt.Rectangle cells = giAux.getOverlappingCells(shapeMBR);

                    for (int col = cells.x; col < cells.x + cells.width; col++) {
                        for (int row = cells.y; row < cells.y + cells.height; row++) {
                            Partition aux = new Partition();
                            aux.cellId = ((partAux.cellId) * SUBCELL_ID) + ((row * giAux.columns) + col + 1);
                            finalOverlapped.add(aux);
                        }
                    }

                }
                return finalOverlapped;

            }else{
                return overlapped;
            }
        }

    }

    /**
     * Filters partitions to remove ones that do not contribute to answer.
     * A partition is pruned if it does not have any points in any of the four
     * skylines.
     * @author Ahmed Eldawy
     *
     */
    public static class EDRJFilter extends DefaultBlockFilter {

        Set<String> validPartitions = new HashSet<String>();

        @Override
        public void configure(Configuration job) {
            // If not set in constructor, read queryRange from the job configuration
            String valids = job.get("valids","");
            String [] aux = valids.split("#");
            for(String id : aux){
                validPartitions.add(id);
                LOG.info(id);
            }
        }

        @Override
        public void selectCells(GlobalIndex<Partition> gIndex,
                                ResultCollector<Partition> output) {
            int count = 0;
            for(Partition p : gIndex){
                if(validPartitions.contains(p.cellId+":"+p.filename)){
                    output.collect(p);
                    count++;
                }
            }

            LOG.info("Selected "+count+" partitions of "+gIndex.size());
        }
    }


    public static <S extends Shape> long EDRJ(Path[] inFiles, Path userOutputPath, OperationsParams params)
            throws IOException, InterruptedException {
        JobConf job = new JobConf(params, EDRJ.class);

        LOG.info("EDRJ journey starts ....");
        FileSystem inFs = inFiles[0].getFileSystem(job);
        Path outputPath = userOutputPath;
        if (outputPath == null) {
            FileSystem outFs = FileSystem.get(job);
            do {
                outputPath = new Path(inFiles[0].getName() + ".EDRJ_" + (int) (Math.random() * 1000000));
            } while (outFs.exists(outputPath));
        }
        FileSystem outFs = FileSystem.get(job);


        ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
        job.setJobName("EDRJ");
        job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));

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
        int EDRJPartitioningGridFactor = params.getInt(PartitioiningFactor, 20);
        int num_cells = (int) Math.max(1,
                total_size * EDRJPartitioningGridFactor / outFs.getDefaultBlockSize(outputPath));
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

        GlobalIndex<Partition> gIndex1 = SpatialSite.getGlobalIndex(inFiles[0].getFileSystem(job), inFiles[0]);


        GlobalIndex<Partition> gIndex2 = SpatialSite.getGlobalIndex(inFiles[1].getFileSystem(job), inFiles[1]);

        Double epsilon = Double.parseDouble(job.get("epsilon", "-1"));

        String validsPartitions = "";
        Vector<Partition> finalPartitions = new Vector<Partition>();
        for(Partition part2 : gIndex2){
            int count = 0;
            for(Partition part1 : gIndex1){
                if(part1.getMinDistance(part2)<= epsilon){
                    validsPartitions += "#" + part1.cellId+":"+part1.filename;
                    count++;
                }
            }
            if(count>0){
                validsPartitions += "#" + (part2.cellId+":"+part2.filename);
                finalPartitions.add(part2);
            }
        }

        Rectangle gIndex2MBR = gIndex2.getMBR();
        PartitionInfo partInfo = new PartitionInfo(gIndex2MBR.x1,gIndex2MBR.y1,gIndex2MBR.x2,gIndex2MBR.y2);
        for (Partition auxR : gIndex2) {
            partInfo.shapes.add(auxR.clone());
            LOG.info(auxR.cellId+":"+auxR.recordCount);
        }

        OperationsParams.setShape(job, PartitionGrid, partInfo);
        job.set("valids",validsPartitions);
        job.setClass(SpatialSite.FilterClass, EDRJFilter.class, BlockFilter.class);


        TextInputFormat inputFormat = new TextInputFormat();
        inputFormat.configure(job);

        job.setMapperClass(EDRJMap1.class);
        job.setMapOutputKeyClass(Partition.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(EDRJReduce1.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        ShapeLineInputFormat.setInputPaths(job, inFiles[1]);

        do {
            outputPath = new Path(userOutputPath.getName() + ".EDRJ1_" + (int) (Math.random() * 1000000));
        } while (outFs.exists(outputPath));

        TextOutputFormat.setOutputPath(job, outputPath);

        RunningJob runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();

        Map<String,String> qtrees = new HashMap<String,String>();

        FileStatus[] stats = outFs.listStatus(outputPath);
        for (int i = 0; i < stats.length; ++i) {
            String fileName = outputPath.toString() + "/"
                + stats[i].getPath().getName();
            LOG.info(fileName);
            if (stats[i].getPath().getName().contains("part")) {
                // read in cell stat info
                BufferedInputStream is = new BufferedInputStream(outFs.open(new Path(fileName)));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                // parse data
                for (String line = br.readLine(); line != null; line = br
                    .readLine()) {
                    LOG.info(line);
                    String[] data = line.split("\t");
                    qtrees.put(data[0],data[1]);
                }

                outFs.delete(stats[i].getPath(), true);

            }

        }

        outFs.deleteOnExit(outputPath);

        job.set("QTREES",TextSerializerHelper.serializeMap(new Text(),qtrees).toString());

        //STEP 2

        //if(true) throw new InterruptedException("we") ;

        TextInputFormat.setInputPaths(job, inFiles);

        job.setMapperClass(EDRJMap2.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IndexedText.class);

        job.setReducerClass(EDRJReduce2.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class,
            LongWritable.class, IntWritable.class);

        ShapeLineInputFormat.setInputPaths(job, inFiles);

        outputPath = userOutputPath;

        TextOutputFormat.setOutputPath(job, outputPath);

        runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();

        Map<String,String> sizes = new HashMap<String,String>();

        stats = outFs.listStatus(outputPath);
        for (int i = 0; i < stats.length; ++i) {
            String fileName = outputPath.toString() + "/"
                + stats[i].getPath().getName();
            LOG.info(fileName);
            if (stats[i].getPath().getName().contains("test")) {
                // read in cell stat info
                BufferedInputStream is = new BufferedInputStream(outFs.open(new Path(fileName)));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                // parse data
                for (String line = br.readLine(); line != null; line = br
                    .readLine()) {
                    String[] data = line.split("\t");
                    LOG.info(line);
                    sizes.put(data[0],data[1]);
                }

                outFs.delete(stats[i].getPath(), true);

            }

        }

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
        long resultSize = EDRJ(inputPaths, outputPath, params);
        long t2 = System.currentTimeMillis();
        System.out.println("Total time: " + (t2 - t1) + " millis");
        System.out.println("Result size: " + resultSize);
    }

}
