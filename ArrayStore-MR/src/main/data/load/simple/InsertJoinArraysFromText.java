package data.load.simple;

import hadoop.io.types.SimpleChunkDataWritable;
import hadoop.io.types.array.ArrayOfIntsWritable;
import hadoop.io.types.array.ArrayOfWritables;
import hadoop.utils.Constants;

import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections15.list.FastArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import array.utils.ArrayAnyDUtils;
import array.utils.JArrayUtils;
import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayOfDoublesWritable;
import edu.umd.cloud9.io.map.HashMapWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;

/**
 * @author Khoa
 * This class is responsible for inserting data (from plain text) of 2 arrays 
 * into collocated chunks. For each chunk's range, the data from each array will be 
 * saved as a pair of writables, which is the value of the key-value sequence file.
 * The key will be the range of the chunk.
 */
public class InsertJoinArraysFromText extends Configured implements Tool{
  public static final Logger LOG = Logger.getLogger(InsertJoinArraysFromText.class);
  
  public final static byte CS = 0;
  public final static byte TR = 1;
  
  public final static String DIM_SIZE_F = "dimSizes";
  public final static String CHUNK_SIZE_F = "chunkSizes";
  public final static String CHUNK_OVERLAP_F = "chunkOverlaps";
  
  private static class MyArrayExpandMapper extends Mapper<LongWritable, Text, LongWritable, PairOfWritables<PairOfInts, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> {

    private final static LongWritable KEY = new LongWritable();
    private final static PairOfWritables<PairOfInts, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>> 
            VALUE = new PairOfWritables<PairOfInts, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>();
    private final static PairOfInts ARRAY_TYPE = new PairOfInts();
    private final static PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable> CELL = new PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>();
    private final static ArrayOfIntsWritable COORD = new ArrayOfIntsWritable();
    private final static ArrayOfDoublesWritable ATTR = new ArrayOfDoublesWritable();
    private final static int DIM_LENGTH = 6;
    
//    private int[] dimSizes = null;
    private int[] chunkSizes = null;
    private int[][] dims = null;
    private int[] chunkOverlaps = null;
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      dims = JArrayUtils.parseIntArrays(context.getConfiguration().get(DIMENSIONS), ",", ":");
      chunkSizes = JArrayUtils.toInts(context.getConfiguration().get(CHUNK_SIZE_F).split(","));
      chunkOverlaps = JArrayUtils.toInts(context.getConfiguration().get(CHUNK_OVERLAP_F).split(","));
      
      VALUE.set(ARRAY_TYPE, CELL);
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        long[] coords = JArrayUtils.toLongs(fields, 0, dims.length);
        coords[3] = coords[3] % 8640000; ////Using t, doy, year,
        COORD.setArray(JArrayUtils.toInts(coords));
        ATTR.setArray(JArrayUtils.toDoubles(fields, dims.length, fields.length));
        CELL.set(COORD, ATTR);
        
        //Fill in main data
        if(fields.length == 15) {
          ARRAY_TYPE.set(CS, Constants.CODE_CHUNK_DATA);
        } else if (fields.length == 18) {
          ARRAY_TYPE.set(TR, Constants.CODE_CHUNK_DATA);
        }
        KEY.set(ArrayAnyDUtils.getChunkIndex(COORD.getArray(), dims, chunkSizes));
//        LOG.info("COORDS: " + JArrayUtils.toString(COORD.getArray(), ",") + ", CHUNK " + KEY.get());
        context.write(KEY, VALUE);
        
        //Fill in overlap data
        ARRAY_TYPE.set(ARRAY_TYPE.getLeftElement(), Constants.CODE_CHUNK_OVERLAP);
        long[] overlapIndices = ArrayAnyDUtils.getOverlapChunkIndices(COORD.getArray(), dims, chunkSizes, chunkOverlaps);
        if(overlapIndices != null) {
          for(long overlapIndex: overlapIndices) {
            KEY.set(overlapIndex);
            context.write(KEY, VALUE);
          }
        }
    }
  }

  private static class MyArraySimpleMapper extends Mapper<LongWritable, Text, LongWritable, ArrayOfWritables<SimpleChunkDataWritable>> {

    private final static LongWritable KEY = new LongWritable();
    private final static FastArrayList<ArrayOfWritables<SimpleChunkDataWritable>> DATA = new FastArrayList<ArrayOfWritables<SimpleChunkDataWritable>>();
    private static ArrayOfWritables<SimpleChunkDataWritable> CORE = null;
    private static ArrayOfWritables<SimpleChunkDataWritable> OVERLAP = null;
    private static SimpleChunkDataWritable TR_CHUNK = null;
    private long[] chunkSizes = null;
    private long[][] dims = null;
    private long[] chunkOverlaps = null;
    
//    Calendar mapStart = null;
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
//      mapStart = Calendar.getInstance();
      dims = JArrayUtils.parseLongArrays(context.getConfiguration().get(DIMENSIONS), ",", ":");
      chunkSizes = JArrayUtils.toLongs(context.getConfiguration().get(CHUNK_SIZE_F).split(","));
      chunkOverlaps = JArrayUtils.toLongs(context.getConfiguration().get(CHUNK_OVERLAP_F).split(","));
      DATA.add(new ArrayOfWritables<SimpleChunkDataWritable>(new SimpleChunkDataWritable[] {new SimpleChunkDataWritable(dims.length, 11), null, null, null}));
      DATA.add(new ArrayOfWritables<SimpleChunkDataWritable>(new SimpleChunkDataWritable[] {null, new SimpleChunkDataWritable(dims.length, 11), null, null}));
      DATA.add(new ArrayOfWritables<SimpleChunkDataWritable>(new SimpleChunkDataWritable[] {null, null, new SimpleChunkDataWritable(dims.length, 14), null}));
      DATA.add(new ArrayOfWritables<SimpleChunkDataWritable>(new SimpleChunkDataWritable[] {null, null, null, new SimpleChunkDataWritable(dims.length, 14)}));
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        
        String[] fields = value.toString().split(",");

        long[] coords = JArrayUtils.toLongs(fields, 0, dims.length);
        double[] attrs = JArrayUtils.toDoubles(fields, dims.length, fields.length);
                
        //Fill in main data
        if(fields.length == 15) {
          CORE = DATA.get(Constants.CODE_CS_DATA);
          OVERLAP = DATA.get(Constants.CODE_CS_OVERLAP);
          CORE.get(Constants.CODE_CS_DATA).set(0, coords, attrs);
          OVERLAP.get(Constants.CODE_CS_OVERLAP).set(0, coords, attrs);
        } else if (fields.length == 18) {
          CORE = DATA.get(Constants.CODE_TR_DATA);
          OVERLAP = DATA.get(Constants.CODE_TR_OVERLAP);
          CORE.get(Constants.CODE_TR_DATA).set(0, coords, attrs);
          OVERLAP.get(Constants.CODE_TR_OVERLAP).set(0, coords, attrs);
        }
        KEY.set(ArrayAnyDUtils.getChunkIndex(coords, dims, chunkSizes));
        context.write(KEY, CORE);
        
        //Fill in overlap data
        long[] overlapIndices = ArrayAnyDUtils.getOverlapChunkIndices(coords, dims, chunkSizes, chunkOverlaps);
        if(overlapIndices != null) {
          for(long overlapIndex: overlapIndices) {
            KEY.set(overlapIndex);
            context.write(KEY, OVERLAP);
          }
        }
    }
    
//    @Override
//    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
//        InterruptedException {
//      LOG.info("Map running time: " + (Calendar.getInstance().getTimeInMillis() - mapStart.getTimeInMillis()));
//    }
  }
  
  private static class MyArraySimpleCombiner extends Reducer<LongWritable, ArrayOfWritables<SimpleChunkDataWritable>,
    LongWritable, ArrayOfWritables<SimpleChunkDataWritable>> {
    ArrayOfWritables<SimpleChunkDataWritable> VALUE = null;
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      VALUE = new ArrayOfWritables<SimpleChunkDataWritable>(new SimpleChunkDataWritable[] {
          new SimpleChunkDataWritable(4, 11),
          new SimpleChunkDataWritable(4, 11),
          new SimpleChunkDataWritable(4, 14),
          new SimpleChunkDataWritable(4, 14)
      });
    }
    
    @Override
    public void reduce(LongWritable key, Iterable<ArrayOfWritables<SimpleChunkDataWritable>> values,
        Context context) throws IOException, InterruptedException {
      Iterator<ArrayOfWritables<SimpleChunkDataWritable>> iter = values.iterator();
      
      for(int i=0; i<VALUE.length(); i++) 
        VALUE.get(i).clear();
      
      while(iter.hasNext()) {
        ArrayOfWritables<SimpleChunkDataWritable> l = iter.next();
        for(int i=0; i<l.length(); i++) {
          if(l.get(i) != null) { //cases where the chunk has no data
            VALUE.get(i).addAll(l.get(i));
          }
        }
      }
         
      context.write(key, VALUE);
    }
  }
  
  private static class MyArraySimpleReducer extends Reducer<LongWritable, ArrayOfWritables<SimpleChunkDataWritable>,
  LongWritable, ArrayOfWritables<SimpleChunkDataWritable>> {
  ArrayOfWritables<SimpleChunkDataWritable> VALUE = null;
  
  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    VALUE = new ArrayOfWritables<SimpleChunkDataWritable>(new SimpleChunkDataWritable[] {
        new SimpleChunkDataWritable(4, 11),
        new SimpleChunkDataWritable(4, 11),
        new SimpleChunkDataWritable(4, 14),
        new SimpleChunkDataWritable(4, 14)
    });
  }
  
  @Override
  public void reduce(LongWritable key, Iterable<ArrayOfWritables<SimpleChunkDataWritable>> values,
      Context context) throws IOException, InterruptedException {
    Iterator<ArrayOfWritables<SimpleChunkDataWritable>> iter = values.iterator();
    for(int i=0; i<VALUE.length(); i++) 
      VALUE.get(i).clear();
    
    while(iter.hasNext()) {
      ArrayOfWritables<SimpleChunkDataWritable> l = iter.next();
      for(int i=0; i<l.length(); i++) {
        if(l.get(i) != null) { //cases where the chunk has no data
          VALUE.get(i).addAll(l.get(i));
        }
      }
    }
    
    context.write(key, VALUE);
  }
}
  
  private static class MyArrayExpandReducer extends Reducer<LongWritable, PairOfWritables<PairOfInts, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>, 
    LongWritable, HashMapWritable<PairOfInts, SortedMapWritable>> {
    
    private static final HashMapWritable<PairOfInts, SortedMapWritable> 
        VALUE = new HashMapWritable<PairOfInts, SortedMapWritable>(); 
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      VALUE.put(new PairOfInts(CS, Constants.CODE_CHUNK_DATA), new SortedMapWritable());
      VALUE.put(new PairOfInts(TR, Constants.CODE_CHUNK_DATA), new SortedMapWritable());
      VALUE.put(new PairOfInts(CS, Constants.CODE_CHUNK_OVERLAP), new SortedMapWritable());
      VALUE.put(new PairOfInts(TR, Constants.CODE_CHUNK_OVERLAP), new SortedMapWritable());
    }
    
    @Override
    public void reduce(LongWritable key, Iterable<PairOfWritables<PairOfInts, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> values,
        Context context) throws IOException, InterruptedException {
       Iterator<PairOfWritables<PairOfInts, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> iter = values.iterator();
       for(SortedMapWritable m: VALUE.values()) {
         m.clear();
       }
       
       while(iter.hasNext()) {
         PairOfWritables<PairOfInts, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>> rec = iter.next();
         SortedMapWritable chunk = null;
         
         chunk = VALUE.get(rec.getLeftElement()); 
         
         //This is because HADOOP reuses object, so simply putting it on the list will not work
         chunk.put(new ArrayOfIntsWritable(rec.getRightElement().getLeftElement().getArray()), 
                   new ArrayOfDoublesWritable(rec.getRightElement().getRightElement().getArray()));
       }
       
       context.write(key, VALUE);
    }
 }

  
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
//  private static final String MAX_DISTANCE = "distance";
  private static final String NUM_REDUCERS = "reducers";
  private static final String DIMENSIONS = "dims";
  private static final String CHUNKS = "chunks";
  private static final String OVERLAPS = "overlaps";
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
//    options.addOption(OptionBuilder.withArgName("distance").hasArg()
//        .withDescription("distance along all dimension for matching").create(MAX_DISTANCE));
    options.addOption(OptionBuilder.withArgName("dims").hasArg()
        .withDescription("").create(DIMENSIONS));
    options.addOption(OptionBuilder.withArgName("chunks").hasArg()
        .withDescription("").create(CHUNKS));
    options.addOption(OptionBuilder.withArgName("overlaps").hasArg()
        .withDescription("").create(OVERLAPS));
    options.addOption(OptionBuilder.withArgName("method").hasArg()
        .withDescription("").create(METHOD));
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

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    //int maxDistance = Integer.parseInt(cmdline.getOptionValue(MAX_DISTANCE));
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
    String dims = cmdline.getOptionValue(DIMENSIONS);
    String chunks = cmdline.getOptionValue(CHUNKS);
    String overlaps = cmdline.getOptionValue(OVERLAPS);

    LOG.info("Tool: " + InsertJoinArraysFromText.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output array to: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);
    LOG.info(" - Dimensions: " + dims);
    LOG.info(" - Chunks: " + chunks);
    LOG.info(" - Overlaps: " + overlaps);
    LOG.info(" - Method: " + method);
    
    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(InsertJoinArraysFromText.class.getSimpleName());
    job.setJarByClass(InsertJoinArraysFromText.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    
    MapFileOutputFormat.setCompressOutput(job, true);
    MapFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    
    if(method.equals(Constants.METHOD_OLD)) {
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(PairOfWritables.class);
      job.setMapperClass(MyArrayExpandMapper.class);
      job.setReducerClass(MyArrayExpandReducer.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(HashMapWritable.class);
    } else {
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(ArrayOfWritables.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(ArrayOfWritables.class);
      job.setMapperClass(MyArraySimpleMapper.class);
      job.setCombinerClass(MyArraySimpleCombiner.class);
      job.setReducerClass(MyArraySimpleReducer.class);
    } 

    // Set configuration data
    job.getConfiguration().set(DIMENSIONS, dims);
    job.getConfiguration().set(CHUNK_SIZE_F, chunks);
    job.getConfiguration().set(CHUNK_OVERLAP_F, overlaps);
    
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
    ToolRunner.run(new InsertJoinArraysFromText(), args);
  }
}
