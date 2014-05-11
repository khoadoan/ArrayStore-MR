package data.load;
import hadoop.io.types.SimpleChunkDataWritable;
import hadoop.io.types.array.ArrayOfIntsWritable;
import hadoop.io.types.array.ArrayOfLongsWritable;
import hadoop.io.types.array.ChunkDoubleDataWritable;
import hadoop.io.types.array.ChunkKeyWritable;
import intersect.hdf.AbstractHDF4;
import intersect.hdf.IntersectHDF4;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import array.utils.ArrayAnyDUtils;
import array.utils.JArrayUtils;
import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayOfDoublesWritable;

public class InsertArrayFromHDF extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(InsertArrayFromHDF.class);

  // Mapper: emit the pair of coordinates and attributes to the same reducer as
  // other pairs that should be in the same chunk.
  private static class MyHDFMapper<T extends IntersectHDF4> extends Mapper<Text, T, LongWritable,  SimpleChunkDataWritable> {

    // Reuse objects to save overhead of object creation.
    private final static LongWritable KEY = new LongWritable(1);
    private final static SimpleChunkDataWritable VALUE = new SimpleChunkDataWritable();
    
//    private final static ArrayOfIntsWritable DIM = new ArrayOfIntsWritable();
//    private final static ArrayOfDoublesWritable ATTRIBUTES = new ArrayOfDoublesWritable();
    
    private static IntersectHDF4 reader = null;
    private static long[] chunkSizes = null;
    private static long[][] dimensionRanges = null;
    private static long[] dimensionSizes = null;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();

      dimensionRanges = JArrayUtils.parseLongArrays(conf.get(DIM_RANGES), DEFAULT_FIELD_DELIMITER, DEFAULT_RANGE_DELIMITER);
      dimensionSizes = new long[dimensionRanges.length];
      for(int i=0; i<dimensionRanges.length; i++) {
        dimensionSizes[i] = dimensionRanges[i][1] - dimensionRanges[i][0] + 1;
      }
      chunkSizes = JArrayUtils.parseLongs(conf.get(CHUNK_SIZES), DEFAULT_FIELD_DELIMITER);
    }
    
    @Override
    public void map(Text key, T value, Context context)
        throws IOException, InterruptedException {
      
      while(value.next()) {
        long[] coordinates = value.getCoordinates();
        double[] attributes = value.getAttributes();
        
        long chunkIndex = ArrayAnyDUtils.getChunkIndex(coordinates, dimensionSizes, chunkSizes);
        //Assign coordinates of the chunk to be zero-based
        JArrayUtils.mod(coordinates, chunkSizes);
        //Ship the cell to same node as all cells within this chunk
        KEY.set(chunkIndex);
        VALUE.clear();
        VALUE.add(coordinates, attributes);
        context.write(KEY, VALUE);
//        recCount++;
      }
//      recTotalLength += value.getLength();
    }
    
//    static long recCount = 0;
//    static long recTotalLength = 0;
//    @Override
//    protected void cleanup(Context context) throws IOException,
//        InterruptedException {
//      throw new InterruptedException("RecCount=" + recCount + "\tRecTotalLength=" + recTotalLength);
//    }
  }
  
  private static class MySeqMapper extends Mapper<ArrayOfLongsWritable, ArrayOfDoublesWritable, LongWritable,  SimpleChunkDataWritable> {

    // Reuse objects to save overhead of object creation.
    private final static LongWritable KEY = new LongWritable(1);
    private final static SimpleChunkDataWritable VALUE = new SimpleChunkDataWritable();
    
//    private final static ArrayOfIntsWritable DIM = new ArrayOfIntsWritable();
//    private final static ArrayOfDoublesWritable ATTRIBUTES = new ArrayOfDoublesWritable();
    
    private static AbstractHDF4 reader = null;
    private static long[] chunkSizes = null;
    private static long[][] dimensionRanges = null;
    private static long[] dimensionSizes = null;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      dimensionRanges = JArrayUtils.parseLongArrays(conf.get(DIM_RANGES), DEFAULT_FIELD_DELIMITER, DEFAULT_RANGE_DELIMITER);
      dimensionSizes = new long[dimensionRanges.length];
      for(int i=0; i<dimensionRanges.length; i++) {
        dimensionSizes[i] = dimensionRanges[i][1] - dimensionRanges[i][0] + 1;
      }
      chunkSizes = JArrayUtils.parseLongs(conf.get(CHUNK_SIZES), DEFAULT_FIELD_DELIMITER);
    }
    
    @Override
    public void map(ArrayOfLongsWritable key, ArrayOfDoublesWritable value, Context context)
        throws IOException, InterruptedException {
      long chunkIndex = ArrayAnyDUtils.getChunkIndex(key.getArray(), dimensionSizes, chunkSizes);
        
      //Assign coordinates of the chunk to be zero-based
      JArrayUtils.mod(key.getArray(), chunkSizes);
      
      //Ship the cell to same node as all cells within this chunk
      KEY.set(chunkIndex);
      VALUE.clear();
      VALUE.add(key.getArray(), value.getArray());
      context.write(KEY, VALUE);
    }
  }
  
  private static class MyCombiner extends Reducer<IntWritable, SimpleChunkDataWritable, IntWritable, SimpleChunkDataWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<SimpleChunkDataWritable> values,
        Context context) throws IOException, InterruptedException {
      SimpleChunkDataWritable value = values.iterator().next();
      value.addAll(values);
      context.write(key, value);
    }
  }

//  // Partitioner:
//  private static class MyPartitioner extends Partitioner<PairOfInts, ArrayOfDoublesWritable> {
//
//    @Override
//    public int getPartition(PairOfInts key, ArrayOfDoublesWritable value, int numReducers) {
//      return super.getPartition();
//    }
//    
//  }
  
  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<LongWritable, SimpleChunkDataWritable, ChunkKeyWritable, ChunkDoubleDataWritable> {

    // Reuse objects.
    private static ChunkKeyWritable KEY = new ChunkKeyWritable();
    private static ChunkDoubleDataWritable VALUE; // = new ChunkDoubleDataWritable(); TODO: reuse object later on
    private static String[] dimensionNames = null;
    private static long[] chunkSizes = null;
    private static long[][] dimensionRanges = null;
    private static long[] dimensionSizes = null;
    private static String[] attributeNames = null;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      dimensionNames = JArrayUtils.parseStrings(conf.get(DIM_NAMES), DEFAULT_FIELD_DELIMITER);
      dimensionRanges = JArrayUtils.parseLongArrays(conf.get(DIM_RANGES), DEFAULT_FIELD_DELIMITER, DEFAULT_RANGE_DELIMITER);
      dimensionSizes = new long[dimensionRanges.length];
      for(int i=0; i<dimensionRanges.length; i++) {
        dimensionSizes[i] = dimensionRanges[i][1] - dimensionRanges[i][0] + 1;
      }
      chunkSizes = JArrayUtils.parseLongs(conf.get(CHUNK_SIZES), DEFAULT_FIELD_DELIMITER);
      attributeNames = JArrayUtils.parseStrings(conf.get(ATTR_NAMES), DEFAULT_FIELD_DELIMITER);
    }

    @Override
    public void reduce(LongWritable key, Iterable<SimpleChunkDataWritable> values,
        Context context) throws IOException, InterruptedException {
//      KEY.setIndex(key.get());
//      KEY.setRanges(ArrayAnyDUtils.getChunkRange(key.get(), dimensionRanges, chunkSizes));
//      
//      VALUE = new ChunkDoubleDataWritable(chunkSizes, dimensionNames, attributeNames);
//      
//      Iterator<SimpleChunkDataWritable> iter = values.iterator();
//      while(iter.hasNext()) {
//        SimpleChunkDataWritable data  = iter.next();
//        for(int i=0; i<data.size(); i++) {
//          long[] coords = data.getCoordinates(i);
//          double[] attrValues = data.getAttributes(i);
//          
//          VALUE.setDouble(attrValues, coords);
//        }
//      }
      
      context.write(KEY, VALUE);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public InsertArrayFromHDF() {}

  private static final String STATISTICS_OUTPUT = "statistics";
  
  private static final String INPUT = "input";
  private static final String NUM_REDUCERS = "numReducers";
  private static final String HDF_READER = "hdfReader";
  
  private static final String ARRAY_NAME = "name";
  private static final String DIM_NAMES = "dimNames";
  private static final String DIM_RANGES = "dimRanges";
  private static final String ATTR_NAMES = "attrNames";
  private static final String CHUNK_SIZES = "chunkSizes";
  private static final String DEFAULT_ARRAY_LOCATION = "arrays/";
  private static final String DEFAULT_FIELD_DELIMITER = ",";
  private static final String DEFAULT_RANGE_DELIMITER = ":";
  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("array name").create(ARRAY_NAME));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));
    options.addOption(OptionBuilder.withArgName("reader").hasArg()
        .withDescription("reader class of HDF").create(HDF_READER));
    options.addOption(OptionBuilder.withArgName("dim").hasArg()
        .withDescription("names of dimensions").create(DIM_NAMES));
    options.addOption(OptionBuilder.withArgName("dim").hasArg()
        .withDescription("ranges of dimensions").create(DIM_RANGES));
    options.addOption(OptionBuilder.withArgName("attribute").hasArg()
        .withDescription("names of attributes").create(ATTR_NAMES));
    options.addOption(OptionBuilder.withArgName("chunk").hasArg()
        .withDescription("dimension sizes of chunk along each dimension").create(CHUNK_SIZES));
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(ARRAY_NAME) || 
          !cmdline.hasOption(HDF_READER) || !cmdline.hasOption(DIM_NAMES) 
            || !cmdline.hasOption(DIM_RANGES) || !cmdline.hasOption(ATTR_NAMES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String arrayName = cmdline.getOptionValue(ARRAY_NAME);
    String outputPath = DEFAULT_ARRAY_LOCATION + arrayName;
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
    
    String hdfReaderClass = cmdline.getOptionValue(HDF_READER);
    String dimNames = cmdline.getOptionValue(DIM_NAMES);
    String dimRanges = cmdline.getOptionValue(DIM_RANGES);
    String attrNames = cmdline.getOptionValue(ATTR_NAMES);
    String chunkSizes = cmdline.getOptionValue(CHUNK_SIZES);


    LOG.info("Tool: " + InsertArrayFromHDF.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output array to: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);
    LOG.info(" - HDF reader class: " + hdfReaderClass);
    LOG.info(" - Dimension Names: " + dimNames);
    LOG.info(" - Ranges of Dimensions: " + dimRanges);
    LOG.info(" - Chunk Sizes: " + chunkSizes);
    LOG.info(" - Attributes: " + attrNames);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(InsertArrayFromHDF.class.getSimpleName());
    job.setJarByClass(InsertArrayFromHDF.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(SimpleChunkDataWritable.class);
    
    job.setOutputKeyClass(ChunkKeyWritable.class);
    job.setOutputValueClass(ChunkDoubleDataWritable.class);

    job.setMapperClass(MyHDFMapper.class);
    
//    job.setCombinerClass(MyCombiner.class);
    //job.setPartitionerClass(MyPartitioner.class);
    job.setReducerClass(MyReducer.class);

    // Set configuration data
    job.getConfiguration().set(HDF_READER, hdfReaderClass);
    job.getConfiguration().set(DIM_NAMES, dimNames);
    job.getConfiguration().set(DIM_RANGES, dimRanges);
    job.getConfiguration().set(ATTR_NAMES, attrNames);
    job.getConfiguration().set(CHUNK_SIZES, chunkSizes);
    
    // Set output format for statistics of the sequence file
//    MultipleOutputs.addNamedOutput(job, STATISTICS_OUTPUT, TextOutputFormat.class, IntWritable.class, IntWritable.class);

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
    ToolRunner.run(new InsertArrayFromHDF(), args);
  }
}