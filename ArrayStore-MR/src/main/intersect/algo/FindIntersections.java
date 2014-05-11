package intersect.algo;

import hadoop.io.types.PairOfWritables;
import hadoop.io.types.SimpleChunkDataWritable;
import hadoop.io.types.array.ArrayOfIntsWritable;
import hadoop.io.types.array.ArrayOfLongsWritable;
import hadoop.io.types.array.ArrayOfWritables;
import hadoop.utils.Constants;

import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import ags.utils.dataStructures.trees.thirdGenKD.DistanceFunction;
import ags.utils.dataStructures.trees.thirdGenKD.KdTree;
import ags.utils.dataStructures.trees.thirdGenKD.NearestNeighborIterator;
import ags.utils.dataStructures.trees.thirdGenKD.SquareEuclideanDistanceFunction;
import array.utils.JArrayUtils;
import cern.colt.Arrays;
import data.load.simple.InsertJoinArraysFromText;
import edu.umd.cloud9.io.array.ArrayOfDoublesWritable;
import edu.umd.cloud9.io.map.HashMapWritable;
import edu.umd.cloud9.io.pair.PairOfInts;

public class FindIntersections extends Configured implements Tool{
  private static final Logger LOG = Logger.getLogger(FindIntersections.class);
  public static final String MAX_DISTANCE = "maxDistance";
  public static final String MATCH_SPEC = "matchSpec";
  public static DistanceFunction DISTANCE_FUNCTION = null;
  
  private static class MyIntersectionsOldMapper extends Mapper<LongWritable, HashMapWritable<PairOfInts, SortedMapWritable>, 
  PairOfInts, PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> {
    private static final PairOfInts TR_DATA = new PairOfInts(InsertJoinArraysFromText.TR, Constants.CODE_CHUNK_DATA);
    private static final PairOfInts TR_OVERLAP = new PairOfInts(InsertJoinArraysFromText.TR, Constants.CODE_CHUNK_OVERLAP);
    private static final PairOfInts CS_DATA = new PairOfInts(InsertJoinArraysFromText.CS, Constants.CODE_CHUNK_DATA);
    //private static final PairOfInts CS_OVERLAP = new PairOfInts(InsertJoinArraysFromText.CS, Constants.CODE_CHUNK_OVERLAP);
    
    private static final PairOfInts KEY = new PairOfInts();
    private static final PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>> VALUE = 
        new PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, 
            PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>(new PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>(), new PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>());
      
    
    int[] spec = null;
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      spec = context.getConfiguration().getInts(MATCH_SPEC);
      DISTANCE_FUNCTION = new SquareEuclideanDistanceFunction();
    }
    
    @Override
    protected void map(
        LongWritable key,
        HashMapWritable<PairOfInts, SortedMapWritable> value,
        Context context) throws IOException,
        InterruptedException {
//      LOG.info("CHUNK NUMBER: " + key.get());
      Calendar start = Calendar.getInstance();
      SortedMapWritable cs = value.get(CS_DATA);
      SortedMapWritable tr = value.get(TR_DATA);
      SortedMapWritable trOverlap = value.get(TR_OVERLAP);
      Iterator<WritableComparable> csIter = cs.keySet().iterator();
      
      //Construct k-d tree of the second array, including overlap
      KdTree<PairOfWritables<WritableComparable, Writable>> trTree = new KdTree<PairOfWritables<WritableComparable, Writable>>(spec.length, tr.size() + trOverlap.size());
      for(Entry<WritableComparable, Writable> trEntry: tr.entrySet()) {
        trTree.addPoint(JArrayUtils.convertToDoubles(((ArrayOfIntsWritable)trEntry.getKey()).getArray()), new PairOfWritables<WritableComparable, Writable>(trEntry.getKey(), trEntry.getValue()));
      }
      
      for(Entry<WritableComparable, Writable> trEntry: trOverlap.entrySet()) {
//        LOG.info("COORDS: " + JArrayUtils.toString(((ArrayOfIntsWritable)trEntry.getKey()).getArray(), ","));
//        LOG.info("ATTRIBUTES: " + JArrayUtils.toString(((ArrayOfDoublesWritable)trEntry.getValue()).getArray(), ",", false));
        trTree.addPoint(JArrayUtils.convertToDoubles(((ArrayOfIntsWritable)trEntry.getKey()).getArray()), new PairOfWritables<WritableComparable, Writable>(trEntry.getKey(), trEntry.getValue()));
      }
      
//      double searchTotalTime = 0;
      for(WritableComparable k: cs.keySet()) {
        Calendar searchStart = Calendar.getInstance();
        ArrayOfIntsWritable csCoords = (ArrayOfIntsWritable) k;
        NearestNeighborIterator<PairOfWritables<WritableComparable, Writable>> matchedPoint = 
            trTree.getNearestNeighborIterator(JArrayUtils.toDoubles(csCoords.getArray()), 1, DISTANCE_FUNCTION);
//        LOG.info("SPEC " + JArrayUtils.toString(csCoords.getArray(), ","));
  
        //If there is a closest point
        if(matchedPoint.hasNext()) {
          PairOfWritables<WritableComparable, Writable> trmmMatchedPoint = matchedPoint.next();
          ArrayOfIntsWritable trmmMatchedCoords = (ArrayOfIntsWritable) trmmMatchedPoint.getLeftElement();
          ArrayOfDoublesWritable trmmMatchedAttrs = (ArrayOfDoublesWritable) trmmMatchedPoint.getRightElement();
          
          //emit only if matched point satisfies the spec constraint
          if(ArrayOfIntsWritable.within(csCoords, trmmMatchedCoords, spec)) {
            ArrayOfDoublesWritable csAttrs = (ArrayOfDoublesWritable)cs.get(csCoords);
//            LOG.info("ORBIT: " + (int)csAttrs.get(0));
            KEY.set(csCoords.get(1) >= 0 ? 1:0, (int)csAttrs.get(0));
            VALUE.getLeftElement().set(csCoords, csAttrs);
            VALUE.getRightElement().set(trmmMatchedCoords, trmmMatchedAttrs);
            context.write(KEY, VALUE);
          }
          
        }
//        searchTotalTime += (Calendar.getInstance().getTimeInMillis() - searchStart.getTimeInMillis());
      }
      
//      long duration = Calendar.getInstance().getTimeInMillis() - start.getTimeInMillis();
//      LOG.info("TIME (msSECONDS): " + duration);
//      LOG.info("SEARCHES: " + cs.size());
//      LOG.info("AVERAGE PER SEARCH: " + searchTotalTime/cs.size());
    }
  }
  
  private static class MyIntersectionsMapper extends Mapper<LongWritable, ArrayOfWritables<SimpleChunkDataWritable>, 
    PairOfInts, PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>> {
    private static final PairOfInts TR_DATA = new PairOfInts(InsertJoinArraysFromText.TR, Constants.CODE_CHUNK_DATA);
    private static final PairOfInts TR_OVERLAP = new PairOfInts(InsertJoinArraysFromText.TR, Constants.CODE_CHUNK_OVERLAP);
    private static final PairOfInts CS_DATA = new PairOfInts(InsertJoinArraysFromText.CS, Constants.CODE_CHUNK_DATA);
    //private static final PairOfInts CS_OVERLAP = new PairOfInts(InsertJoinArraysFromText.CS, Constants.CODE_CHUNK_OVERLAP);
    
    private static final PairOfInts KEY = new PairOfInts();
    private static final PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>> VALUE = 
        new PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, 
            PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>(new PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>(), new PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>());
    
    private static final ArrayOfDoublesWritable csMatchedAttributes = new ArrayOfDoublesWritable(); 
    private static final ArrayOfLongsWritable CS_COORDINATES = new ArrayOfLongsWritable();
    private static final ArrayOfDoublesWritable CS_ATTRIBUTES =  new ArrayOfDoublesWritable();
    
    int[] spec = null;
    
//    private MultipleOutputs mos1, mos2;
//    private long mapKey = Calendar.getInstance().getTimeInMillis();
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      spec = context.getConfiguration().getInts(MATCH_SPEC);
      DISTANCE_FUNCTION = new SquareEuclideanDistanceFunction();
//      mos1 = new MultipleOutputs(context);
//      mos2 = new MultipleOutputs(context);
    }
    
    @Override
    protected void map(
        LongWritable key,
        ArrayOfWritables<SimpleChunkDataWritable> value,
        Context context) throws IOException,
        InterruptedException {
      long mapStart = Calendar.getInstance().getTimeInMillis();
     

      SimpleChunkDataWritable cs = value.get(Constants.CODE_CS_DATA);
      SimpleChunkDataWritable tr = value.get(Constants.CODE_TR_DATA);
      SimpleChunkDataWritable trOverlap = value.get(Constants.CODE_TR_OVERLAP);

      //Construct k-d tree of the second array, including overlap
      KdTree<PairOfWritables<Writable, Writable>> trTree = new KdTree<PairOfWritables<Writable, Writable>>(spec.length, tr.size() + trOverlap.size());
      for(int i=0; i<tr.size(); i++) {
        trTree.addPoint(JArrayUtils.convertToDoubles(tr.getCoordinates(i)), 
            new PairOfWritables<Writable, Writable>(new ArrayOfLongsWritable(tr.getCoordinates(i)), new ArrayOfDoublesWritable(tr.getAttributes(i))));
      }
      
//      LOG.info(tr.size());
//      LOG.info("DIMS SIZE = " + spec.length);
      for(int i=0; i<trOverlap.size(); i++) {
//        LOG.info(i + ": " + JArrayUtils.toString(trOverlap.getCoordinates(i), ","));
//        JArrayUtils.convertToDoubles(trOverlap.getCoordinates(i));
//        new ArrayOfLongsWritable(trOverlap.getCoordinates(i));
//        new ArrayOfDoublesWritable(trOverlap.getAttributes(i));
//        LOG.info("SIZE = " + trOverlap.getCoordinates(i).length);
        trTree.addPoint(JArrayUtils.convertToDoubles(trOverlap.getCoordinates(i)), 
            new PairOfWritables<Writable, Writable>(new ArrayOfLongsWritable(trOverlap.getCoordinates(i)), new ArrayOfDoublesWritable(trOverlap.getAttributes(i))));
      }
      
      for(int i=0; i<cs.size(); i++) {
        NearestNeighborIterator<PairOfWritables<Writable, Writable>> matchedPoint = 
            trTree.getNearestNeighborIterator(JArrayUtils.toDoubles(cs.getCoordinates(i)), 1, DISTANCE_FUNCTION);

        //If there is a closest point
        if(matchedPoint.hasNext()) {
          PairOfWritables<Writable, Writable> trmmMatchedPoint = matchedPoint.next();
          ArrayOfLongsWritable trmmMatchedCoords = (ArrayOfLongsWritable) trmmMatchedPoint.getLeftElement();
          ArrayOfDoublesWritable trmmMatchedAttrs = (ArrayOfDoublesWritable) trmmMatchedPoint.getRightElement();

          //emit only if matched point satisfies the spec constraint
          if(ArrayOfLongsWritable.within(cs.getCoordinates(i), trmmMatchedCoords.getArray(), spec)) {
            csMatchedAttributes.setArray(cs.getAttributes(i));
            KEY.set(cs.getCoordinates(i)[1] >= 0 ? 1:0, (int)cs.getAttributes(i)[2]);
            
            CS_COORDINATES.setArray(cs.getCoordinates(i));
            CS_ATTRIBUTES.setArray(cs.getAttributes(i));
            
            VALUE.getLeftElement().set(CS_COORDINATES, CS_ATTRIBUTES);
            VALUE.getRightElement().set(trmmMatchedCoords, trmmMatchedAttrs);
            context.write(KEY, VALUE);
          }
        }
      }
      
//      mos1.write("maprunningtime", key, new Text(cs.size() + "," + trTree.size() + "," + (Calendar.getInstance().getTimeInMillis() - mapStart)));
    }
    
    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
        InterruptedException {
//      mos2.write("maprunningtimetotal", mapKey, new Text(String.valueOf(Calendar.getInstance().getTimeInMillis() - mapKey)));
//      mos1.close();
//      mos2.close();
    }
  }
    
  private static class MyIntersectionCombiner 
    extends Reducer<PairOfInts, PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>, 
      PairOfInts, PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>> {
    PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>> SMALLEST_VALUE = null;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      SMALLEST_VALUE = new PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>(
          new PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>(new ArrayOfLongsWritable(), new ArrayOfDoublesWritable()), 
          new PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>(new ArrayOfLongsWritable(), new ArrayOfDoublesWritable()));
    }
    
    @Override
    protected void reduce(PairOfInts key,
        Iterable<PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>> values,
        Context context) throws IOException, InterruptedException {
      Iterator<PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>> 
        iter = values.iterator();
      
      double smallestDistance = Double.MAX_VALUE;
      
      while(iter.hasNext()) {
        PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>> 
            value = iter.next();

        double distance = JArrayUtils.distance(value.getLeftElement().getLeftElement().getArray(), value.getRightElement().getLeftElement().getArray(), 0, 3);

        if(distance < smallestDistance) {
          smallestDistance = distance;
//          SMALLEST_VALUE = value;
          SMALLEST_VALUE.getLeftElement().getLeftElement().setArray(value.getLeftElement().getLeftElement().getArray());
          SMALLEST_VALUE.getLeftElement().getRightElement().setArray(value.getLeftElement().getRightElement().getArray());
          SMALLEST_VALUE.getRightElement().getLeftElement().setArray(value.getRightElement().getLeftElement().getArray());
          SMALLEST_VALUE.getRightElement().getRightElement().setArray(value.getRightElement().getRightElement().getArray());
        }
      }
      
      context.write(key, SMALLEST_VALUE);
    }
  }
  
  private static class MyIntersectionOldCombiner 
    extends Reducer<PairOfInts, PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>, 
      PairOfInts, PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> {
    PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>> SMALLEST_VALUE = null;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      SMALLEST_VALUE = new PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>(
          new PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>(new ArrayOfIntsWritable(), new ArrayOfDoublesWritable()), 
          new PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>(new ArrayOfIntsWritable(), new ArrayOfDoublesWritable()));
    }
    
    @Override
    protected void reduce(PairOfInts key,
        Iterable<PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> values,
        Context context) throws IOException, InterruptedException {
      Iterator<PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> 
        iter = values.iterator();
      
      double smallestDistance = Double.MAX_VALUE;
      
      while(iter.hasNext()) {
        PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>> 
            value = iter.next();
  
        double distance = JArrayUtils.distance(value.getLeftElement().getLeftElement().getArray(), value.getRightElement().getLeftElement().getArray(), 0, 3);
  
        if(distance < smallestDistance) {
          smallestDistance = distance;
//          SMALLEST_VALUE = value;
          SMALLEST_VALUE.getLeftElement().getLeftElement().setArray(value.getLeftElement().getLeftElement().getArray());
          SMALLEST_VALUE.getLeftElement().getRightElement().setArray(value.getLeftElement().getRightElement().getArray());
          SMALLEST_VALUE.getRightElement().getLeftElement().setArray(value.getRightElement().getLeftElement().getArray());
          SMALLEST_VALUE.getRightElement().getRightElement().setArray(value.getRightElement().getRightElement().getArray());
        }
      }
      
      context.write(key, SMALLEST_VALUE);
    }
  }
  
  private static class MyIntersectionReducer 
    extends Reducer<PairOfInts, PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>, 
                    PairOfInts, PairOfWritables<PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>, DoubleWritable>> {
    
    PairOfWritables<PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>, DoubleWritable> VALUE = null;
    DoubleWritable DIST = null;
    PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>> SMALLEST_VALUE = null;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      DIST = new DoubleWritable();
      VALUE = new PairOfWritables<PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>, DoubleWritable>();
      SMALLEST_VALUE = new PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>(
          new PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>(new ArrayOfLongsWritable(), new ArrayOfDoublesWritable()), 
          new PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>(new ArrayOfLongsWritable(), new ArrayOfDoublesWritable()));
    }
    
    @Override
    protected void reduce(PairOfInts key,
        Iterable<PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>> values,
        Context context) throws IOException, InterruptedException {
      Iterator<PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>> iter = values.iterator();
      
      double smallestDistance = Double.MAX_VALUE;
      
      while(iter.hasNext()) {
        PairOfWritables<PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>> value = iter.next();

        double distance = JArrayUtils.distance(value.getLeftElement().getLeftElement().getArray(), value.getRightElement().getLeftElement().getArray(), 0, 3);

        if(distance < smallestDistance) {
          smallestDistance = distance;
//          SMALLEST_VALUE = value;
          SMALLEST_VALUE.getLeftElement().getLeftElement().setArray(value.getLeftElement().getLeftElement().getArray());
          SMALLEST_VALUE.getLeftElement().getRightElement().setArray(value.getLeftElement().getRightElement().getArray());
          SMALLEST_VALUE.getRightElement().getLeftElement().setArray(value.getRightElement().getLeftElement().getArray());
          SMALLEST_VALUE.getRightElement().getRightElement().setArray(value.getRightElement().getRightElement().getArray());
        }
      }
      
      DIST.set(smallestDistance);
      VALUE.set(SMALLEST_VALUE, DIST);
      
      context.write(key, VALUE);
    }
  }
  
  private static class MyIntersectionOldReducer 
  extends Reducer<PairOfInts, PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>, 
                  PairOfInts, PairOfWritables<PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>, DoubleWritable>> {
  
  PairOfWritables<PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>, DoubleWritable> VALUE = null;
  DoubleWritable DIST = null;
  PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>> SMALLEST_VALUE = null;
  
  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    DIST = new DoubleWritable();
    VALUE = new PairOfWritables<PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>, DoubleWritable>();
    SMALLEST_VALUE = new PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>(
        new PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>(new ArrayOfIntsWritable(), new ArrayOfDoublesWritable()), 
        new PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>(new ArrayOfIntsWritable(), new ArrayOfDoublesWritable()));
  }
  
  @Override
  protected void reduce(PairOfInts key,
      Iterable<PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> values,
      Context context) throws IOException, InterruptedException {
    Iterator<PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> iter = values.iterator();
    
    double smallestDistance = Double.MAX_VALUE;
    
    while(iter.hasNext()) {
      PairOfWritables<PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>> value = iter.next();

      double distance = JArrayUtils.distance(value.getLeftElement().getLeftElement().getArray(), value.getRightElement().getLeftElement().getArray(), 0, 3);

      if(distance < smallestDistance) {
        smallestDistance = distance;
        SMALLEST_VALUE = value;
        SMALLEST_VALUE.getLeftElement().getLeftElement().setArray(value.getLeftElement().getLeftElement().getArray());
        SMALLEST_VALUE.getLeftElement().getRightElement().setArray(value.getLeftElement().getRightElement().getArray());
        SMALLEST_VALUE.getRightElement().getLeftElement().setArray(value.getRightElement().getLeftElement().getArray());
        SMALLEST_VALUE.getRightElement().getRightElement().setArray(value.getRightElement().getRightElement().getArray());
      }
    }
    
    DIST.set(smallestDistance);
    VALUE.set(SMALLEST_VALUE, DIST);
    
    context.write(key, VALUE);
  }
}
  
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "reducers";
  private static final String METHOD = "method";
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));
    options.addOption(OptionBuilder.withArgName("distance").hasArg()
        .withDescription("specification of matching").create(MATCH_SPEC));
    options.addOption(OptionBuilder.withArgName("method").hasArg()
        .withDescription("method of intersection").create(METHOD));
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }
    
    String method = "";
    if(cmdline.hasOption(METHOD)) {
      method = cmdline.getOptionValue(METHOD);
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(MATCH_SPEC)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
    String matchSpec = cmdline.getOptionValue(MATCH_SPEC);
    
    LOG.info("Tool: " + InsertJoinArraysFromText.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output array to: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);
    LOG.info(" - match spec: " + matchSpec);
    
    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(InsertJoinArraysFromText.class.getSimpleName());
    job.setJarByClass(InsertJoinArraysFromText.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(PairOfWritables.class);
    
    job.setOutputKeyClass(PairOfInts.class);
    job.setOutputValueClass(PairOfWritables.class);
    
    if(method.equals(Constants.METHOD_OLD)) {
      job.setMapperClass(MyIntersectionsOldMapper.class);
      job.setCombinerClass(MyIntersectionOldCombiner.class);
      job.setReducerClass(MyIntersectionOldReducer.class);
    } else {
      job.setMapperClass(MyIntersectionsMapper.class);
      job.setCombinerClass(MyIntersectionCombiner.class);
      job.setReducerClass(MyIntersectionReducer.class);
      
//      MultipleOutputs.addNamedOutput(job, "maprunningtime", TextOutputFormat.class,
//          IntWritable.class, Text.class);
//      MultipleOutputs.addNamedOutput(job, "maprunningtimetotal", TextOutputFormat.class,
//          IntWritable.class, Text.class);
    }
    
    // Set configuration data
    job.getConfiguration().set(MATCH_SPEC, matchSpec);
    
    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }
  
  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new FindIntersections(), args);
  }

}
