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
import edu.umn.cs.spatialHadoop.indexing.VoronoiRandomPartitioner;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;

/**
 * An implementation of Spatial Join MapReduce as described in S. Zhang, J. Han,
 * Z. Liu, K. Wang, and Z. Xu. KNNJ: Parallelizing spatial join with MapReduce
 * on clusters. In CLUSTER, pages 1–8, New Orleans, LA, Aug. 2009. The map
 * function partitions data into grid cells and the reduce function makes a
 * plane-sweep over each cell.
 *
 * @author eldawy
 */
public class KNNJRVoronoi {

    /**
     * Class logger
     */
    private static final Log LOG = LogFactory.getLog(KNNJRVoronoi.class);
    private static final String PartitionGrid = "KNNJ.PartitionGrid";
    private static final String InactiveMode = "KNNJ.InactiveMode";
    private static final String isFilterOnlyMode = "DJ.FilterOnlyMode";
    private static final String JoiningThresholdPerOnce = "DJ.JoiningThresholdPerOnce";
    public static boolean isReduceInactive = false;
    public static boolean isSpatialJoinOutputRequired = true;
    public static boolean isFilterOnly = false;
    public static int joiningThresholdPerOnce = 50000;
    private static double SAMPLE_RATIO = 0.02;

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

        public List<Partition> getOverlappingCells(Point point) {

            List<Partition> overlapped = new ArrayList<Partition>();

            double minDistance = Double.MAX_VALUE;
            Partition thePart = null;
            for(Partition partAux : shapes){
                double distance = 0;
                if(partAux.pivot!=null){
                    distance = partAux.pivot.distanceTo(point);
                } else {
                    distance = partAux.getMinDistance(point.getMBR());
                }

                if(distance<minDistance){
                    thePart = partAux;
                    minDistance = distance;
                }
            }
            overlapped.add(thePart);
            return overlapped;

        }



        public Partition getPartition(int cellId) {
            for(Partition partAux : shapes){
                if(partAux.cellId == cellId){
                    return partAux;
                }
            }
            return null;
        }

    }


    /**
     * Separa los archivos de entrada en 2 listas asociandoles la cell id adecuada
     *
     * @author Ahmed Eldawy
     */
    public static class KNNJMap1 extends MapReduceBase implements Mapper<Rectangle, Text, Partition, IndexedText> {
        private Shape shape;
        private IndexedText outputValue = new IndexedText();
        private PartitionInfo gridInfo;
        private Path[] inputFiles;
        private InputSplit currentSplit;
        Random rand;
        @Override
        public void configure(JobConf job) {
            super.configure(job);
            // Retrieve grid to use for partitioning
            gridInfo = (PartitionInfo) OperationsParams.getShape(job, PartitionGrid);
            // Create a stock shape for deserializing lines
            shape = SpatialSite.createStockShape(job);
            // Get input paths to determine file index for every record

            inputFiles = FileInputFormat.getInputPaths(job);

            rand = new Random();

        }

        @Override
        public void map(Rectangle cellMbr, Text value, OutputCollector<Partition, IndexedText> output,
                        Reporter reporter) throws IOException {

            if(rand.nextFloat()>SAMPLE_RATIO){
                return;
            }

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

                if(shape instanceof Point){
                    List<Partition> cells = gridInfo.getOverlappingCells((Point)shape);

                    for (Partition part : cells) {

                        output.collect(part, outputValue);
                    }
                }else {
                    List<Partition> cells = gridInfo.getOverlappingCells(shape_mbr);

                    for (Partition part : cells) {

                        output.collect(part, outputValue);
                    }
                }
            }
        }
    }

    // Recrea las listas de puntos R y S y calcula KNNJ (RxS)
    public static class KNNJReduce1<S extends Shape> extends MapReduceBase
        implements Reducer<Partition, IndexedText, IntWritable, VoronoiRandomPartitioner> {
        /**
         * Class logger
         */
        private static final Log KNNJReduceLOG = LogFactory.getLog(KNNJReduce1.class);

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
        IntWritable aux;
        MultipleOutputs mos;


        @Override
        public void configure(JobConf job) {
            super.configure(job);
            shape = (S) SpatialSite.createStockShape(job);
            shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
            inputFileCount = FileInputFormat.getInputPaths(job).length;
            k = job.getInt("k", 1);
            KNNJReduceLOG.info("configured the reduced task");
            aux = new IntWritable();
            mos = new MultipleOutputs(job);

        }

        @Override
        public void reduce(Partition cellId, Iterator<IndexedText> values,
                           final OutputCollector<IntWritable, VoronoiRandomPartitioner> output, Reporter reporter) throws IOException {

            LOG.info("Start reduce() logic now !!!");
            long t1 = System.currentTimeMillis();

            // Partition retrieved shapes (values) into lists for each file
            Vector<S> shapes  = new Vector<S>();

            while (values.hasNext()) {

                IndexedText t = values.next();
                S s = (S) shape.clone();
                s.fromText(t.text);
                shapes.add(s);

            }
            Point finalShapes[] = new Point[shapes.size()];
            shapes.toArray(finalShapes);


            VoronoiRandomPartitioner qt = new VoronoiRandomPartitioner();
            qt.createFromPoints(cellId,finalShapes,(int)Math.floor(shapesThresholdPerOnce*SAMPLE_RATIO));

            qt.setReadOnly(true);
            int count[] = new int[qt.getPivotInfo().length];
            for(Shape shape : shapes){
                int partition = qt.overlapPartition(shape);
                count[partition]++;
            }

            for(int n = 0; n < count.length; n++){
                long finalCellId = ((long) (cellId.cellId) * SUBCELL_ID) + n;
                mos.getCollector("minSizes", reporter).collect(finalCellId, count[n]);
            }

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
    public static class KNNJMap2 extends MapReduceBase implements Mapper<Rectangle, Text, LongWritable, IndexedText> {
        private Shape shape;
        private IndexedText outputValue = new IndexedText();
        private PartitionInfo gridInfo;
        private LongWritable cellId = new LongWritable();
        private Path[] inputFiles;
        private InputSplit currentSplit;
        private Map<String, VoronoiRandomPartitioner> qtrees;
        private Map<String, Integer> minSizes;


        @Override
        public void configure(JobConf job) {
            super.configure(job);
            // Retrieve grid to use for partitioning
            gridInfo = (PartitionInfo) OperationsParams.getShape(job, PartitionGrid);
            // Create a stock shape for deserializing lines
            shape = SpatialSite.createStockShape(job);
            // Get input paths to determine file index for every record
            inputFiles = FileInputFormat.getInputPaths(job);

            String sizesAux = job.get("QTREES", null);
            if(sizesAux!=null){
                Map<String,String> aux = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),aux);
                qtrees = new HashMap<String, VoronoiRandomPartitioner>();
                for(String key : aux.keySet()) {
                    VoronoiRandomPartitioner qp = new VoronoiRandomPartitioner();
                    qp.fromText(new Text(aux.get(key).getBytes()));
                    qtrees.put(key,qp);
                }
            }
            sizesAux = job.get("MINSIZES", null);
            if(sizesAux!=null){
                Map<String,String> aux = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),aux);
                minSizes = new HashMap<String, Integer>();
                for(String key : aux.keySet()) {
                    minSizes.put(key,Integer.parseInt(aux.get(key)));
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

                List<Partition> cells = null;
                if(shape instanceof Point)
                    cells = gridInfo.getOverlappingCells((Point)shape);
                else
                    cells = gridInfo.getOverlappingCells(shape_mbr);

                if(cells.size()!=1) {
                    throw new NotImplementedException("TODO");
                }

                for (final Partition part : cells) {

                    VoronoiRandomPartitioner qp = qtrees.get(part.cellId+"");
                    if(shape instanceof Point){
                        qp.setReadOnly(true);

                        Integer x = qp.overlapPartition(shape);
                        if(qp==null){
                            x = 0;
                        }

                        long finalCellId = ((long) (part.cellId) * SUBCELL_ID) + x.longValue();
                        cellId.set(finalCellId);
                        try {
                            output.collect(cellId, outputValue);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        throw new NotImplementedException("TODO");
                    }

                }

            }
        }
    }

    // Recrea las listas de puntos R y S y calcula KNNJ (RxS)
    public static class KNNJReduce2<S extends Shape> extends MapReduceBase
            implements Reducer<LongWritable, IndexedText, S, ArrayWritable> {
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
        LongWritable aux;
        MultipleOutputs mos;
        private Map<String, VoronoiRandomPartitioner> qtrees;
        private Map<String, Integer> minSizes;


        @Override
        public void configure(JobConf job) {
            super.configure(job);
            shape = (S) SpatialSite.createStockShape(job);
            shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
            inputFileCount = FileInputFormat.getInputPaths(job).length;
            k = job.getInt("k", 1);
            KNNJReduceLOG.info("configured the reduced task");
            aux = new LongWritable();
            mos = new MultipleOutputs(job);


            String sizesAux = job.get("QTREES", null);
            if(sizesAux!=null){
                Map<String,String> aux = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),aux);
                qtrees = new HashMap<String, VoronoiRandomPartitioner>();
                for(String key : aux.keySet()) {
                    VoronoiRandomPartitioner qp = new VoronoiRandomPartitioner();
                    qp.fromText(new Text(aux.get(key).getBytes()));
                    qtrees.put(key,qp);
                }
            }

            sizesAux = job.get("MINSIZES", null);
            if(sizesAux!=null){
                Map<String,String> aux = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),aux);
                minSizes = new HashMap<String, Integer>();
                for(String key : aux.keySet()) {
                    minSizes.put(key,Integer.parseInt(aux.get(key)));
                }
            }

        }

        @Override
        public void reduce(LongWritable cellId, Iterator<IndexedText> values,
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
                KNNJReduceLOG.info(cellId.get() + ": Joining (" + shapeLists[0].size() + " X " + shapeLists[1].size() + ")...");

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

                //shapeLists[1].clear();
            }

            long parent = (long)Math.floor(((double)cellId.get())/((double)SUBCELL_ID));
            int child = (int)(cellId.get()%SUBCELL_ID);
            VoronoiRandomPartitioner qp = qtrees.get(parent+"");
            PivotInfo pivot = qp.getPivotInfo()[child];
            for(Shape shape : shapeLists[1]){
                if(shape instanceof Point){

                    Point point = (Point) shape;
                    double distance = pivot.distanceTo(point);
                    if(distance>pivot.maxDis){
                        pivot.maxDis = distance;
                    }
                    if(distance<pivot.minDis){
                        pivot.minDis = distance;
                    }

                }
            }
            aux.set(shapeLists[1].size());
            mos.getCollector("test", reporter).collect(cellId, this.aux);
            mos.getCollector("minDis", reporter).collect(cellId, pivot.maxDis);
            mos.getCollector("maxDis", reporter).collect(cellId, pivot.minDis);


            long t2 = System.currentTimeMillis();
            LOG.info("Reducer finished in: " + (t2 - t1) + " millis");

        }

        @Override
        public void close() throws IOException {
            mos.close();
            super.close();
        }
    }

    /**
     * The map class maps each object to all cells it overlaps with.
     *
     * @author Ahmed Eldawy
     */
    public static class KNNJMap3<S extends Shape> extends MapReduceBase implements Mapper<Rectangle, Text, LongWritable, IndexedText> {
        private S shape;
        private IndexedText outputValue = new IndexedText();
        private PartitionInfo gridInfo;
        private LongWritable cellId = new LongWritable();
        private Path[] inputFiles;
        private InputSplit currentSplit;
        private int k;
        private Text tempText = new Text();
        private Text tempShapeText = new Text();
        private Map<String,String> sizes = null;

        private int elementCount = 0;
        private double minMaxDistance = 0;
        private Map<String, VoronoiRandomPartitioner> qtrees;

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
            String sizesAux = job.get("SIZES", null);
            if(sizesAux!=null){
                sizes = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),sizes);
            }

            Map<String,String> maxDis = null;
            Map<String,String> minDis = null;
            sizesAux = job.get("MAXDIS", null);
            if(sizesAux!=null){
                maxDis = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),maxDis);
            }

            sizesAux = job.get("MINDIS", null);
            if(sizesAux!=null){
                minDis = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),minDis);
            }

            sizesAux = job.get("QTREES", null);
            if(sizesAux!=null){
                Map<String,String> aux = new HashMap<String, String>();
                TextSerializerHelper.consumeMap(new Text(sizesAux),aux);
                qtrees = new HashMap<String, VoronoiRandomPartitioner>();
                for(String key : aux.keySet()) {
                    VoronoiRandomPartitioner qp = new VoronoiRandomPartitioner();
                    qp.fromText(new Text(aux.get(key).getBytes()));
                    qtrees.put(key,qp);
                    long cellId = Long.parseLong(key);
                    for(int n = 0; n < qp.getPivotInfo().length; n++){
                        PivotInfo pivot = qp.getPivotInfo()[n];
                        long finalCellId = (cellId * SUBCELL_ID) + n;
                        String auxString = maxDis.get(finalCellId+"");
                        if(auxString!=null) {
                            pivot.maxDis = Double.parseDouble(auxString);
                            pivot.minDis = Double.parseDouble(minDis.get(finalCellId + ""));
                        } else {
                            pivot.maxDis = 0;
                            pivot.minDis = 0;
                        }

                    }
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

            if (outputValue.index == 0) {

                tempText.set(value);
                tempShapeText.clear();
                outputValue.text = value;
                shape.fromText(tempText);

                shape.toText(tempShapeText);
                outputValue.text = tempShapeText;

                Rectangle shape_mbr = shape.getMBR();

                //Remove tab
                tempText.set(tempText.getBytes(), 1, tempText.getLength() - 1);

                final long auxCellId = TextSerializerHelper.consumeLong(tempText, ',');
                int newSize = TextSerializerHelper.consumeInt(tempText, ',');
                double maxSize = TextSerializerHelper.consumeDouble(tempText, ',');

                if(newSize==k) {
                    outputValue.text.append(new byte[] {(byte)','}, 0, 1);

                    TextSerializerHelper.serializeDouble(maxSize, outputValue.text, '\0');
                }


                // Do a reference point technique to avoid processing the same
                // record twice
                if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {
                    Rectangle shapeMBR = shape.getMBR();
                    if (shapeMBR == null)
                        return;

                    double radius;
                    double radiusAux = 0;
                    minMaxDistance = Double.MAX_VALUE;

                    if(newSize>0){
                        radius = maxSize;
                    }else{

                        VoronoiRandomPartitioner qp = qtrees.get((long)Math.floor(((double)auxCellId)/((double)SUBCELL_ID))+"");
                        PivotInfo pivot = qp.getPivotInfo()[(int)(auxCellId%SUBCELL_ID)];
                        radius = pivot.distanceTo(shapeMBR.getCenterPoint());
                    }

                    elementCount = 0;

                    //LOG.info("radius "+radius );
                    //LOG.info("size "+ topKNN.heap.size() );

                    while (newSize + elementCount < k) {
                        Rectangle shapeMBR2 = shape.getMBR().clone();
                        radiusAux += radius;
                        shapeMBR2.x1 -= radiusAux;
                        shapeMBR2.y1 -= radiusAux;
                        shapeMBR2.x2 += radiusAux;
                        shapeMBR2.y2 += radiusAux;
                        elementCount = 0;

                        List<Partition> cells = gridInfo.getOverlappingCells(shapeMBR2);
                        for (final Partition part : cells) {

                            VoronoiRandomPartitioner qp = qtrees.get(part.cellId+"");
                            qp.overlapPartitions(shapeMBR2,new ResultCollector<Integer>() {

                                @Override
                                public void collect(Integer x) {
                                    long finalCellId = ((long)(part.cellId) * SUBCELL_ID) + x.longValue();
                                    if (finalCellId == auxCellId)
                                        return;
                                    String count = sizes.get(finalCellId+"");
                                    Integer thisCount = Integer.parseInt(count==null?"0":count);
                                    elementCount += thisCount;
                                    if(thisCount>=k)
                                        minMaxDistance = part.getMaxDistance(shape.getMBR())<minMaxDistance?part.getMaxDistance(shape.getMBR()):minMaxDistance;

                                }
                            });



                        }

                        if(shapeMBR2.contains(gridInfo)){
                            minMaxDistance = radiusAux;
                            break;
                        }
                    }

                    if(radiusAux==0) {
                        radiusAux = radius;
                    }
                    else{
                        radiusAux = minMaxDistance;
                    }


                    shapeMBR.x1 -= radiusAux;
                    shapeMBR.y1 -= radiusAux;
                    shapeMBR.x2 += radiusAux;
                    shapeMBR.y2 += radiusAux;

                    List<Partition> cells = gridInfo.getOverlappingCells(shapeMBR);


                    for (final Partition part : cells) {

                        VoronoiRandomPartitioner qp = qtrees.get(part.cellId+"");
                        qp.overlapPartitions(shapeMBR,new ResultCollector<Integer>() {

                            @Override
                            public void collect(Integer x) {
                                long finalCellId = ((long)(part.cellId) * SUBCELL_ID) + x.longValue();
                                cellId.set(finalCellId);
                                if (finalCellId == auxCellId)
                                    return;
                                if(sizes.get(finalCellId+"")==null)
                                    return;

                                try {
                                    output.collect(cellId, outputValue);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        });



                    }

                }
            } else {

                tempText.set(value);
                outputValue.text = value;
                shape.fromText(tempText);
                Rectangle shape_mbr = shape.getMBR();
                // Do a reference point technique to avoid processing the same
                // record twice
                if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.x1, shape_mbr.y1)) {

                    List<Partition> cells = null;
                    if(shape instanceof Point)
                        cells = gridInfo.getOverlappingCells((Point)shape);
                    else
                        cells = gridInfo.getOverlappingCells(shape_mbr);

                    if(cells.size()==0) {
                        throw new NotImplementedException("TODO");
                    }

                    for (final Partition part : cells) {

                        VoronoiRandomPartitioner qp = qtrees.get(part.cellId+"");
                        if(shape instanceof Point){
                            qp.setReadOnly(true);

                            Integer x = qp.overlapPartition(shape);

                            long finalCellId = ((long) (part.cellId) * SUBCELL_ID) + x.longValue();
                            cellId.set(finalCellId);
                            try {
                                output.collect(cellId, outputValue);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else {
                            throw new NotImplementedException("TODO");
                        }

                    }

                }
            }
        }
    }

    public static class KNNJReduce3<S extends Shape> extends MapReduceBase
            implements Reducer<LongWritable, IndexedText, S, ArrayWritable> {
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
        public void reduce(LongWritable cellId, Iterator<IndexedText> values,
                           final OutputCollector<S, ArrayWritable> output, Reporter reporter) throws IOException {

            LOG.info("Start reduce() logic now !!!");
            long t1 = System.currentTimeMillis();

            List<Double> maxRadius = new Vector<Double>();

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
                    if(t.index==0){
                        if(t.text.getLength()>0){
                            t.text.set(t.text.getBytes(), 1, t.text.getLength() - 1);
                            maxRadius.add(TextSerializerHelper.consumeDouble(t.text,'\0'));
                        }else{
                            maxRadius.add(Double.MAX_VALUE);
                        }
                    }
                } while (values.hasNext() /*
									 * && shapeLists[1].size() <
									 * shapesThresholdPerOnce
									 */);

                // Perform spatial join between the two lists
                KNNJReduceLOG.info("Joining (" + shapeLists[0].size() + " X " + shapeLists[1].size() + ")...");
                if(shapeLists[0].isEmpty())
                    break;
                SpatialAlgorithms.KNNJoin_planeSweep(shapeLists[0], maxRadius, shapeLists[1], cellId.get(), k,
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

                //shapeLists[1].clear();
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

            SpatialAlgorithms.TOPKNN<Shape> topKNN = new SpatialAlgorithms.TOPKNN<Shape>(shape,k);

            //Remove tab
            if(tempText.getLength()>0){
                tempText.set(tempText.getBytes(), 1, tempText.getLength() - 1);
                topKNN.fromText(tempText);
            }

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
        private PartitionInfo grid;
        private int shapesThresholdPerOnce;

        private Shape stockShape;
        private int k;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            grid = (PartitionInfo) OperationsParams.getShape(job, PartitionGrid);
            stockShape = SpatialSite.createStockShape(job);
            shapesThresholdPerOnce = OperationsParams.getJoiningThresholdPerOnce(job, JoiningThresholdPerOnce);
            inputFileCount = FileInputFormat.getInputPaths(job).length;
            k = job.getInt("k", 1);
            KNNJReduceLOG.info("configured the reduced task");
        }

        @Override
        public void reduce(Shape shape, Iterator<SpatialAlgorithms.TOPKNN<Shape>> values,
                           final OutputCollector<Shape, SpatialAlgorithms.TOPKNN<Shape>> output, Reporter reporter) throws IOException {

            if (values.hasNext()) {
                SpatialAlgorithms.TOPKNN<Shape> auxTopKNN = values.next();
                if (!values.hasNext()) {
                    output.collect(shape, auxTopKNN);
                } else {
                    SpatialAlgorithms.TOPKNN<Shape> finalTopKNN = new SpatialAlgorithms.TOPKNN<Shape>(stockShape,k);

                    SpatialAlgorithms.ShapeNN<Shape> auxShape;
                    while ((auxShape = auxTopKNN.heap.pop()) != null) {
                        finalTopKNN.add(auxShape.shape, auxShape.dist);
                    }
                    do {
                        auxTopKNN = values.next();
                        while ((auxShape = auxTopKNN.heap.pop()) != null) {
                            finalTopKNN.add(auxShape.shape, auxShape.dist);
                        }
                    }
                    while (values.hasNext());

                    output.collect(shape,finalTopKNN);
                }
            }

        }



    }

    public static <S extends Shape> long KNNJ(Path[] inFiles, Path userOutputPath, OperationsParams params)
            throws IOException, InterruptedException {
        JobConf job = new JobConf(params, KNNJRVoronoi.class);

        LOG.info("KNNJ journey starts ....");
        long t1 = System.currentTimeMillis();

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
        job.setJobName("KNNJ 1 " + job.getInt("k", 1));
        job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));

        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        if (job.getBoolean("output", true))
            job.setOutputFormat(TextOutputFormat.class);
        else
            job.setOutputFormat(NullOutputFormat.class);


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
        Rectangle gIndex1MBR = gIndex1.getMBR();
        for (Partition auxR : gIndex1) {
            LOG.info(auxR.cellId+":"+auxR.recordCount);
        }
        LOG.info("::::");
        GlobalIndex<Partition> gIndex2 = SpatialSite.getGlobalIndex(inFiles[1].getFileSystem(job), inFiles[1]);
        Rectangle gIndex2MBR = gIndex2.getMBR();
        PartitionInfo partInfo = new PartitionInfo(gIndex2MBR.x1,gIndex2MBR.y1,gIndex2MBR.x2,gIndex2MBR.y2);
        for (Partition auxR : gIndex2) {
            partInfo.shapes.add(auxR.clone());
            LOG.info(auxR.cellId+":"+auxR.recordCount);
        }

        OperationsParams.setShape(job, PartitionGrid, partInfo);

        job.setMapperClass(KNNJMap1.class);
        job.setMapOutputKeyClass(Partition.class);
        job.setMapOutputValueClass(IndexedText.class);

        job.setReducerClass(KNNJReduce1.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, "minSizes", TextOutputFormat.class,
            LongWritable.class, IntWritable.class);

        ShapeLineInputFormat.setInputPaths(job, inFiles[1]);

        do {
            outputPath = new Path(userOutputPath.getName() + ".KNNJ1_" + (int) (Math.random() * 1000000));
        } while (outFs.exists(outputPath));

        TextOutputFormat.setOutputPath(job, outputPath);

        RunningJob runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();

        Map<String,String> qtrees = new HashMap<String,String>();
        Map<String,String> minSizes = new HashMap<String,String>();


        FileStatus[] stats = outFs.listStatus(outputPath);
        for (int i = 0; i < stats.length; ++i) {
            String fileName = outputPath.toString() + "/"
                + stats[i].getPath().getName();
            //LOG.info(fileName);
            if (stats[i].getPath().getName().contains("part")) {
                // read in cell stat info
                BufferedInputStream is = new BufferedInputStream(outFs.open(new Path(fileName)));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                // parse data
                for (String line = br.readLine(); line != null; line = br
                    .readLine()) {
                    LOG.info("PART: "+ line);
                    String[] data = line.split("\t");
                    qtrees.put(data[0],data[1]);
                }

                outFs.delete(stats[i].getPath(), true);

            }
            if (stats[i].getPath().getName().contains("minSizes")) {
                // read in cell stat info
                BufferedInputStream is = new BufferedInputStream(outFs.open(new Path(fileName)));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                // parse data
                for (String line = br.readLine(); line != null; line = br
                    .readLine()) {
                    LOG.info(line);
                    String[] data = line.split("\t");
                    minSizes.put(data[0],data[1]);
                }

                outFs.delete(stats[i].getPath(), true);

            }

        }

        outFs.deleteOnExit(outputPath);
        job.set("MINSIZES",TextSerializerHelper.serializeMap(new Text(),minSizes).toString());
        job.set("QTREES",TextSerializerHelper.serializeMap(new Text(),qtrees).toString());

        long t2 = System.currentTimeMillis();

        //STEP 2
        job.setJobName("KNNJ 2 " + job.getInt("k", 1));


        //if(true) throw new InterruptedException("we") ;

        TextInputFormat.setInputPaths(job, inFiles);

        job.setMapperClass(KNNJMap2.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IndexedText.class);

        job.setReducerClass(KNNJReduce2.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class,
            LongWritable.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "maxDis", TextOutputFormat.class,
            LongWritable.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "minDis", TextOutputFormat.class,
            LongWritable.class, DoubleWritable.class);

        ShapeLineInputFormat.setInputPaths(job, inFiles);

        do {
            outputPath = new Path(userOutputPath.getName() + ".KNNJ2_" + (int) (Math.random() * 1000000));
        } while (outFs.exists(outputPath));

        TextOutputFormat.setOutputPath(job, outputPath);

        runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();

        outFs.deleteOnExit(outputPath);

        Map<String,String> sizes = new HashMap<String,String>();
        Map<String,String> maxDis = new HashMap<String,String>();
        Map<String,String> minDis = new HashMap<String,String>();


        stats = outFs.listStatus(outputPath);
        for (int i = 0; i < stats.length; ++i) {
            String fileName = outputPath.toString() + "/"
                + stats[i].getPath().getName();
            //LOG.info(fileName);
            if (stats[i].getPath().getName().contains("test")) {
                // read in cell stat info
                BufferedInputStream is = new BufferedInputStream(outFs.open(new Path(fileName)));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                // parse data
                for (String line = br.readLine(); line != null; line = br
                    .readLine()) {
                    String[] data = line.split("\t");
                    //LOG.info(line);
                    sizes.put(data[0],data[1]);
                }

                outFs.delete(stats[i].getPath(), true);

            }

            if (stats[i].getPath().getName().contains("maxDis")) {
                // read in cell stat info
                BufferedInputStream is = new BufferedInputStream(outFs.open(new Path(fileName)));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                // parse data
                for (String line = br.readLine(); line != null; line = br
                    .readLine()) {
                    String[] data = line.split("\t");
                    //LOG.info(line);
                    maxDis.put(data[0],data[1]);
                }

                outFs.delete(stats[i].getPath(), true);

            }

            if (stats[i].getPath().getName().contains("minDis")) {
                // read in cell stat info
                BufferedInputStream is = new BufferedInputStream(outFs.open(new Path(fileName)));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                // parse data
                for (String line = br.readLine(); line != null; line = br
                    .readLine()) {
                    String[] data = line.split("\t");
                    LOG.info(line);
                    minDis.put(data[0],data[1]);
                }

                outFs.delete(stats[i].getPath(), true);

            }

        }

        for(String the_string : maxDis.keySet()){
            LOG.info(the_string + " : " + sizes.get(the_string) + " " + maxDis.get(the_string) + " " + minDis.get(the_string));
        }

        job.set("SIZES",TextSerializerHelper.serializeMap(new Text(),sizes).toString());
        job.set("MAXDIS",TextSerializerHelper.serializeMap(new Text(),maxDis).toString());
        job.set("MINDIS",TextSerializerHelper.serializeMap(new Text(),minDis).toString());
        job.set("QTREES",TextSerializerHelper.serializeMap(new Text(),qtrees).toString());

        //STEP 3
        long t3 = System.currentTimeMillis();

        job.setJobName("KNNJ 3 " + job.getInt("k", 1));

        job.setMapperClass(KNNJMap3.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IndexedText.class);

        job.setReducerClass(KNNJReduce3.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        Path[] inputFiles3 =  new Path[2];
        inputFiles3[0] = outputPath;
        inputFiles3[1] = inFiles[1];
        ShapeLineInputFormat.setInputPaths(job, inputFiles3);

        job.setLong(org.apache.hadoop.mapreduce.lib.input.
            FileInputFormat.SPLIT_MINSIZE, 128*1024*1024);

        do {
            outputPath = new Path(userOutputPath.getName() + ".KNNJ3_" + (int) (Math.random() * 1000000));
        } while (outFs.exists(outputPath));

        TextOutputFormat.setOutputPath(job, outputPath);

        runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();

        //outFs.deleteOnExit(outputPath);
        long t4 = System.currentTimeMillis();

        //if(true) throw new InterruptedException("we") ;
        job.setJobName("KNNJ 4 " + job.getInt("k", 1));

        job.setMapperClass(KNNJMap4.class);

        Shape stockShape = params.getShape("shape");

        job.setMapOutputKeyClass(stockShape.getClass());
        job.setMapOutputValueClass(SpatialAlgorithms.TOPKNN.class);

        job.setReducerClass(KNNJReduce4.class);
        job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

        job.setInputFormat(ShapeLineInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        job.setLong(org.apache.hadoop.mapreduce.lib.input.
            FileInputFormat.SPLIT_MINSIZE, 128*1024*1024);

        inputFiles3[1] = outputPath;

        ShapeLineInputFormat.setInputPaths(job, inputFiles3);

        TextOutputFormat.setOutputPath(job, userOutputPath);

        runningJob = JobClient.runJob(job);
        runningJob.waitForCompletion();

        long t5 = System.currentTimeMillis();
        System.out.println("PRE STEP: " + (t2 - t1) + " millis");
        System.out.println("STEP 1: " + (t3 - t2) + " millis");
        System.out.println("STEP 2: " + (t4 - t3) + " millis");
        System.out.println("STEP 3: " + (t5 - t4) + " millis");


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
