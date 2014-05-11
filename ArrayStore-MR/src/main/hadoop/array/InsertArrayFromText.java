package hadoop.array;

import hadoop.io.types.array.ArrayOfIntsWritable;
import hadoop.io.types.array.ArrayOfLongsWritable;
import hadoop.utils.Constants;

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
import edu.umd.cloud9.io.pair.PairOfWritables;

public class InsertArrayFromText extends Configured implements Tool{
  private static final Logger LOG = Logger.getLogger(InsertArrayFromText.class);
    
  /**
   * Accept a CSV text version of cells, each of which is a line. And emit the chunk index of the cell, along with
   * the cell's content, including coordinates and attributes.
   * @author Khoa
   *
   */
  private static class MyArrayParititionMapper extends Mapper<LongWritable, Text, 
        LongWritable, PairOfWritables<IntWritable, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>> {

    private final static LongWritable KEY = new LongWritable();
    private final static PairOfWritables<IntWritable, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>> 
                                      VALUE = new PairOfWritables<IntWritable, PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>>();
    private final static IntWritable ARRAY_TYPE = new IntWritable();
    private long[][] dimRanges = null;
    private long[] chunkSizes = null;
    private long[] chunkOverlaps = null;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      dimRanges = JArrayUtils.parseLongArrays(context.getConfiguration().get(Constants.DIMENSION_STR), ",", ":");
      chunkSizes = JArrayUtils.toLongs(context.getConfiguration().get(Constants.CHUNK_STR).split(","));
      chunkOverlaps = JArrayUtils.toLongs(context.getConfiguration().get(Constants.OVERLAP_STR).split(","));
      
      VALUE.set(ARRAY_TYPE, new PairOfWritables<ArrayOfLongsWritable, ArrayOfDoublesWritable>(new ArrayOfLongsWritable(), new ArrayOfDoublesWritable()));
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        
        long[] coords = JArrayUtils.toLongs(fields, 0, dimRanges.length);
        double[] attrs = JArrayUtils.toDoubles(fields, dimRanges.length, fields.length);
        
        //Fill in main data
        ARRAY_TYPE.set(Constants.CODE_CHUNK_DATA);
        VALUE.getRightElement().getLeftElement().setArray(coords);
        VALUE.getRightElement().getRightElement().setArray(attrs);

        KEY.set(ArrayAnyDUtils.getChunkIndex(coords, dimRanges, chunkSizes));
        context.write(KEY, VALUE);
        
        //Fill in overlap data
        ARRAY_TYPE.set(Constants.CODE_CHUNK_OVERLAP);
        long[] overlapIndices = ArrayAnyDUtils.getOverlapChunkIndices(coords, dimRanges, chunkSizes, chunkOverlaps);
        if(overlapIndices != null) {
          for(long overlapIndex: overlapIndices) {
            KEY.set(overlapIndex);
            context.write(KEY, VALUE);
          }
        }
    }
  }

  private static class MyArrayPartitionReducer extends Reducer<LongWritable, PairOfWritables<IntWritable, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>, 
    LongWritable, HashMapWritable<IntWritable, SortedMapWritable>> {
    
    private static final HashMapWritable<IntWritable, SortedMapWritable> 
        VALUE = new HashMapWritable<IntWritable, SortedMapWritable>(); 
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      VALUE.put(new IntWritable(Constants.CODE_CHUNK_DATA), new SortedMapWritable());
      VALUE.put(new IntWritable(Constants.CODE_CHUNK_OVERLAP), new SortedMapWritable());
    }
    
    @Override
    public void reduce(LongWritable key, Iterable<PairOfWritables<IntWritable, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> values,
        Context context) throws IOException, InterruptedException {
       Iterator<PairOfWritables<IntWritable, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>>> iter = values.iterator();
       
       for(SortedMapWritable m: VALUE.values()) {
         m.clear();
       }
       
       while(iter.hasNext()) {
         PairOfWritables<IntWritable, PairOfWritables<ArrayOfIntsWritable, ArrayOfDoublesWritable>> rec = iter.next();
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
        .withDescription("").create(Constants.DIMENSION_STR));
    options.addOption(OptionBuilder.withArgName("chunks").hasArg()
        .withDescription("").create(Constants.CHUNK_STR));
    options.addOption(OptionBuilder.withArgName("overlaps").hasArg()
        .withDescription("").create(Constants.OVERLAP_STR));
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
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
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;
    String dims = cmdline.getOptionValue(Constants.DIMENSION_STR);
    String chunks = cmdline.getOptionValue(Constants.CHUNK_STR);
    String overlaps = cmdline.getOptionValue(Constants.OVERLAP_STR);

    LOG.info("Tool: " + InsertArrayFromText.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output array to: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(InsertArrayFromText.class.getSimpleName());
    job.setJarByClass(InsertArrayFromText.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    
    MapFileOutputFormat.setCompressOutput(job, true);
    MapFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(PairOfWritables.class);
    
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(HashMapWritable.class);

    job.setMapperClass(MyArrayParititionMapper.class);
    job.setReducerClass(MyArrayPartitionReducer.class);

    // Set configuration data
    job.getConfiguration().set(Constants.DIMENSION_STR, dims);
    job.getConfiguration().set(Constants.CHUNK_STR, chunks);
    job.getConfiguration().set(Constants.OVERLAP_STR, overlaps);
    
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
    ToolRunner.run(new InsertArrayFromText(), args);
  }
}
