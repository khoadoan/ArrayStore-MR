package intersect.data;

import intersect.hdf.IntersectHDF4;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.log4j.Logger;

import data.utils.ZipFileExtracter;

public class IntersectSFPacker {
  private static Logger LOG = Logger.getLogger(IntersectSFPacker.class);
  private static IntersectHDF4 value = null;
  public static void main(String... args) throws Exception {
   String baseDirName = args[0];
   String readerClass = args[1];
   String sequenceFileName = args[2];
//   String remoteHadoopFS = args[4];
//   String remoteDir = args[5];

   String localHadoopFS = "file:///";
   SequenceFile.Writer writer = createSequenceFile(sequenceFileName, readerClass, localHadoopFS);
   File baseDir = new File(baseDirName);
   
   System.out.println(baseDir.getName() + " contains " + baseDir.list().length + " directories.");
   for(File hdfDir: baseDir.listFiles(new FileFilter() {
       @Override
       public boolean accept(File pathname) {
         return pathname.isDirectory();
       }
   })) {
     System.out.println("Using Directory " + hdfDir.getName() + "...");
     for(File zipFile: hdfDir.listFiles()) {
       //Unzip file to current directory
       String[] extractedFilenames = ZipFileExtracter.extract(zipFile.getAbsolutePath(), ".");
       for(String hdfFilename: extractedFilenames) {
         File hdfFile = new File(hdfFilename);
         if(hdfFilename.toLowerCase().endsWith("hdf")) {
           System.out.println("\t\t\t\t- Writing " + hdfFile.getName());
           writeToSequenceFile(readerClass, writer, hdfFile);
         }
         //Delete extracted file
         hdfFile.delete();
       }
     }
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

 public static SequenceFile.Writer createSequenceFile(String sequenceFileName, String readerClass, String hadoopFS) throws IOException, ClassNotFoundException {
   Path path = new Path(sequenceFileName);
   Configuration conf = new Configuration();
   SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
       SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new BZip2Codec()),
       SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Class.forName(readerClass)));
   conf.set("fs.defaultFS", hadoopFS);
   return writer;
 }
 
 public static void writeToSequenceFile(String className, SequenceFile.Writer writer, File dataFile) throws IOException {
   Text key = new Text();
   if(value == null)
     value = IntersectHDF4.getReader(className);

   if ((dataFile != null) && (dataFile.exists())) {
      key.set(dataFile.getName());
      
      value.open(dataFile.getName());
      if(value.getLength() > 0) {
        writer.append(key, value);
      } else {
        System.out.println("Empty " + dataFile.getName());
      }
      value.close();
   }
}
}
