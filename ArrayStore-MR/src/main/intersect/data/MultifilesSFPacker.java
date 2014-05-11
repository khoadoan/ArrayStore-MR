package intersect.data;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class MultifilesSFPacker {
   private static Logger LOG = Logger.getLogger(MultifilesSFPacker.class);
   
   public static void main(String... args) throws IOException {
    String baseDirName = args[0];
    String sequenceFileName = args[1];
    String remoteHadoopFS = args[2];
    String remoteDir = args[3];

    String localHadoopFS = "file:///";
    SequenceFile.Writer writer = createSequenceFile(sequenceFileName, localHadoopFS);
    File baseDir = new File(baseDirName);
    
    System.out.println(baseDir.getName() + " contains " + baseDir.list().length + " entries.");
    for(File dataFile: baseDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.getName().toLowerCase().endsWith(".hdf");
        }
    })) {
      System.out.println("Writing " + dataFile.getName() + "...");
      writeToSequenceFile(writer, dataFile);
    }
    
    IOUtils.closeStream(writer);
    
    // Write to HDFS
    //File sequenceFile = new File(sequenceFileName);
    //copySequenceFile(sequenceFileName, remoteDir, remoteHadoopFS);
    
  }
  
  public static void copySequenceFile(String from, String to, String remoteHadoopFS) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", remoteHadoopFS);
    FileSystem fs = FileSystem.get(conf);

    Path localPath = new Path(from);
    Path hdfsPath = new Path(to);
    boolean deleteSource = true;

    fs.copyFromLocalFile(deleteSource, localPath, hdfsPath);
 }

  public static SequenceFile.Writer createSequenceFile(String sequenceFileName, String hadoopFS) throws IOException {
    Path path = new Path(sequenceFileName);
    Configuration conf = new Configuration();
    SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
        //SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new BZip2Codec()),
        SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(BytesWritable.class));
    conf.set("fs.defaultFS", hadoopFS);
    return writer;
  }
  
  public static void writeToSequenceFile(SequenceFile.Writer writer, File dataFile) throws IOException {
    Text key = new Text();
    BytesWritable value = new BytesWritable();

    if ((dataFile != null) && (dataFile.exists())) {
       key.set(dataFile.getName());
       byte[] data = FileUtils.readFileToByteArray(dataFile);
       value.set(data, 0, data.length);
       writer.append(key, value);
       System.out.println("\t" + data.length + " bytes.");
    }
 }
}
