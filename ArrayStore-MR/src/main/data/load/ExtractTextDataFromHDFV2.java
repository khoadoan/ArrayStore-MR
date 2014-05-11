package data.load;

import hadoop.io.types.SimpleChunkDataWritable;
import hadoop.io.types.array.ArrayOfIntsWritable;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import array.utils.ArrayAnyDUtils;
import array.utils.JArrayUtils;
import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayOfDoublesWritable;

public class ExtractTextDataFromHDFV2 extends Configured implements Tool{
  private static final Logger LOG = Logger.getLogger(InsertArrayFromHDF.class);

  // Mapper: emit the pair of coordinates and attributes to the same reducer as
  // other pairs that should be in the same chunk.
  private static class MyHDFMapper<T extends IntersectHDF4> extends Mapper<Text, T, Text,  Text> {

    // Reuse objects to save overhead of object creation.
    private final static Text KEY = new Text();
    private final static Text VALUE = new Text();
    private final static StringBuilder TVALUE = new StringBuilder();
    private final static int MAX = 500;
    
    @Override
    public void map(Text key, T value, Context context)
        throws IOException, InterruptedException {
      int c = MAX;
      KEY.set(key.toString());
      while(value.next()) {
        TVALUE.append(JArrayUtils.toString(value.getRecord(), ",", false));
        c--;
        if(c == 0) {
          VALUE.set(TVALUE.toString());
          context.write(KEY, VALUE);
          //Reset
          c = MAX;
          TVALUE.delete(0, TVALUE.length());
        }
      }
      
      if(TVALUE.length() > 0) {
        VALUE.set(TVALUE.toString());
        context.write(KEY, VALUE);
      }
    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<Text, Text, NullWritable, Text> {
    // Reuse objects.
    private static NullWritable KEY = NullWritable.get();
    
    @Override
    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      Iterator<Text> iter = values.iterator();
      while(iter.hasNext()) {
        context.write(KEY, iter.next());
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ExtractTextDataFromHDFV2() {
    // TODO Auto-generated constructor stub
  }
  
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";
  private static final String HDF_READER = "hdfReader";
 

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));
    options.addOption(OptionBuilder.withArgName("reader").hasArg()
        .withDescription("reader class of HDF").create(HDF_READER));
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(HDF_READER) || !cmdline.hasOption(OUTPUT)) {
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
    
    String hdfReaderClass = cmdline.getOptionValue(HDF_READER);
    
    LOG.info("Tool: " + ExtractTextDataFromHDFV2.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);
    LOG.info(" - HDF reader class: " + hdfReaderClass);
    
    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTextDataFromHDFV2.class.getSimpleName());
    job.setJarByClass(ExtractTextDataFromHDFV2.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyHDFMapper.class);
    
    job.setReducerClass(MyReducer.class);

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
    ToolRunner.run(new ExtractTextDataFromHDFV2(), args);
  }
}
