package data.utils;

import hadoop.io.types.array.ChunkKeyWritable;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.pig.data.TupleFactory;

public class MapFileStatistics {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private MapFileStatistics() {}

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("args: [path] [step size - in bytes]");
      System.exit(-1);
    }

    String f = args[0];

    int step = Integer.parseInt(args[1]); 
    
    boolean useLocal = args.length >= 3 && args[2].equals("local") ? true : false;

    if (useLocal) {
      System.out.println("Reading from local filesystem");
    }

    FileSystem fs = useLocal? FileSystem.getLocal(new Configuration()) : FileSystem.get(new Configuration());
    Path p = new Path(f);

    Map<Integer, Integer> sizeHistogram = null;
    if (fs.getFileStatus(p).isDir()) {
      //readSequenceFilesInDir(p, fs, step);
    } else {
      sizeHistogram = readSequenceFile(p, fs, step);
    }
    
    printMap(sizeHistogram);
  }
  
  private static void printMap(Map<Integer, Integer> m) {
    for(Integer bar: m.keySet()) {
      System.out.println(bar + ": " + m.get(bar));
    }
  }

  private static Map<Integer, Integer> readSequenceFile(Path path, FileSystem fs, int step) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());
    
    Map<Integer, Integer> sizeHistogram = new TreeMap<Integer, Integer>();
    
    ChunkKeyWritable key;
    LongWritable value;
    
    try {
      key = new ChunkKeyWritable();
      value = new LongWritable();
      
      long previousBytesOffset = 0;
      
      while (reader.next(key, value)) {
        long currentBytesOffset = value.get();
        int numberOfBytes = (int)(currentBytesOffset - previousBytesOffset);
        
        System.out.println(currentBytesOffset + ": " + numberOfBytes);
        
        int bar = numberOfBytes / step;
        int counts = 1;
        if(sizeHistogram.containsKey(bar)) {
          counts = sizeHistogram.get(bar) + 1;
        }
        sizeHistogram.put(bar, counts++);
        
        previousBytesOffset = currentBytesOffset;
      }
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return sizeHistogram;
  }

//  private static int readSequenceFilesInDir(Path path, FileSystem fs, int max) {
//    int n = 0;
//    try {
//      FileStatus[] stat = fs.listStatus(path);
//      for (int i = 0; i < stat.length; ++i) {
//        n += readSequenceFile(stat[i].getPath(), fs ,max);
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//    System.out.println(n + " records read in total.");
//    return n;
//  }
}
