package intersect.hdf;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.HObject;

import org.apache.commons.lang.ArrayUtils;

import array.Attribute;
import array.utils.JArrayUtils;

public abstract class AbstractHDF4 {
  public static final byte TINT = 0;
  public static final byte TBYTE = 1;
  public static final byte TSHORT = 2;
  public static final byte TLONG = 3;
  public static final byte TFLOAT = 4;
  public static final byte TDOUBLE = 5;
  
//  public static void main(String... args) throws IOException {
//    ArrayOfIntsWritable key = new ArrayOfIntsWritable();
//    ArrayOfDoublesWritable value = new ArrayOfDoublesWritable();
//    
//    String baseDirName = args[0];
//    String sequenceFileName = args[1];
//    String readerClass = args[2];
//    
//    AbstractHDF4 reader = getReader(readerClass);
//    File baseDir = new File(baseDirName);
//    
//    Path path = new Path(sequenceFileName);
//    Configuration conf = new Configuration();
//    SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
//        //SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new BZip2Codec()),
//        SequenceFile.Writer.keyClass(ArrayOfIntsWritable.class), SequenceFile.Writer.valueClass(ArrayOfDoublesWritable.class));
//    conf.set("fs.defaultFS", "file:///");
//    
//    for(File dataFile: baseDir.listFiles(new FileFilter() {
//        @Override
//        public boolean accept(File arg0) {
//          return arg0.getName().toLowerCase().endsWith(".hdf");
//        }
//      })) {
//      System.out.println("Writing " + dataFile.getName());
//      reader.open(dataFile.getAbsolutePath());
//      while(reader.next()) {
//        key.setArray(reader.currentCoordinates());
//        value.setArray(reader.currentAttributes());
//        writer.append(key, value);
//      }
//      reader.close();
//    }
      
//    IOUtils.closeStream(writer);
//  }
  
  private static FileFormat fileFormat = null;
  
  public static FileFormat openFile4Read(String fname) throws Exception {
    if(fileFormat == null) {
      fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF4);
    }
    FileFormat f = fileFormat.open(fname, FileFormat.READ);
    
    if(f == null) {
      throw new RuntimeException("Failed to open file: " + fname);
    }
    
    f.open();
    return f;
  }
  
  public static void printGroup(Group g, String indent) throws Exception
  {
      if (g == null)
          return;

      List members = g.getMemberList();

      int n = members.size();
      indent += "    ";
      HObject obj = null;
      for (int i=0; i<n; i++)
      {
          obj = (HObject)members.get(i);
          System.out.println(indent+obj);
          if (obj instanceof Group)
          {
              printGroup((Group)obj, indent);
          }
      }
  }
  
  public static Map<String, double[]> readDoubleA(FileFormat f, String... parentGroupPath) {
    Map<String, double[]> dataRead = new HashMap<String, double[]>();
    Group g = (Group)((javax.swing.tree.DefaultMutableTreeNode)f.getRootNode()).getUserObject();
    
    for(String currentGroupName: parentGroupPath) {
      List members = g.getMemberList();
      for(Object memberObj: members) {
        HObject member = (HObject)memberObj;
        if(member.getName().equals(currentGroupName)) {
          g = (Group)memberObj;
          break;
        }
      }
    }
    
    for(Object memberObj: g.getMemberList()) {
      Dataset ds = (Dataset) memberObj;
      try {
        Object dso = ds.read();
        double[] data = null;
        if(dso instanceof Vector){
          Vector v = (Vector)dso;
          if(v.get(0) instanceof float[]) {
            data = JArrayUtils.convertToDoubles((float[])v.get(0));
          } else if(v.get(0) instanceof short[]) {
            data = JArrayUtils.convertToDoubles((short[])v.get(0));
          } else if(v.get(0) instanceof byte[]) {
            data = JArrayUtils.convertToDoubles((byte[])v.get(0));
          } else if(v.get(0) instanceof float[]) {
            data = JArrayUtils.convertToDoubles((float[])v.get(0));
          } else {
            data = (double[])v.get(0);
          }
        } else if (dso instanceof float[]) {
          data = JArrayUtils.convertToDoubles((float[])dso);
        } else if (dso instanceof short[]) {
          data = JArrayUtils.convertToDoubles((short[])dso);
        } else if (dso instanceof byte[]) {
          data = JArrayUtils.convertToDoubles((byte[])dso);
        } else {
          data = (double[])dso;
        }
        
        dataRead.put(ds.getName(), data);
      } catch (OutOfMemoryError e) {
        throw new RuntimeException("Out of memory while reading " + ds.getName(), e);
      } catch (Exception e) {
        throw new RuntimeException("Error while reading " + ds.getName(), e);
      }
    }
    
    return dataRead;
  }
  
 
  
  public static Group getGroup(Group root, String... path) {
    Group currentGroup = root;
    for(String name: path) {
      for(Object memberObj: currentGroup.getMemberList()) {
        if(memberObj instanceof Group) {
          Group dg = (Group)memberObj;
          if(dg.getName().equals(name)) {
            currentGroup = dg;
            break;
          }
        }
      }
    }
    
    return currentGroup;
  }
  
  public static void readGroupDatasets(Group group, Map<String, Attribute> data, String[] names) throws OutOfMemoryError, Exception {
    for(Object memberObj: group.getMemberList()) {
      if(memberObj instanceof Dataset) {
        Dataset ds = (Dataset)memberObj;
        if(ds.getRank() <= 0) {
          ds.init();
        }
        
        if(data.containsKey(ds.getName()))
          throw new RuntimeException("Duplicate Attribute");
        
        //Only read datasets that are specified (or all if the names are null)
        if(names == null || ArrayUtils.contains(names, ds.getName())) {
          Object dso = ds.read();
          if(dso instanceof Vector){
            Vector v = (Vector)dso;
            dso = v.get(0);
          } 
          
          //Only read numeric fields
          if(! (dso instanceof String[])) {
            data.put(ds.getName(), new Attribute(ds.getDims(), dso));
          }
        }
      }
    }
  }
  
  public static int[] getDims(Group group, String name) {
    for(Object memberObj: group.getMemberList()) {
      if(memberObj instanceof Dataset) {
        Dataset ds = (Dataset)memberObj;
        if(ds.getRank() <= 0) {
          ds.init();
        }
        if(ds.getName().equals(name)) {
          long[] dsDims = ds.getDims();
          int[] dims = new int[dsDims.length];
          for(int i=0; i<dsDims.length; i++) {
            dims[i] = (int)dsDims[i];
          }
          return dims;
        }
      }
    }
    return null;
  }
  
  public static void readGroup2DDatasets(Group group, Map<String, Object> data, String... names) throws OutOfMemoryError, Exception {
    for(Object memberObj: group.getMemberList()) {
      if(memberObj instanceof Dataset) {
        Dataset ds = (Dataset)memberObj;
        if(ds.getRank() <= 0) {
          ds.init();
        }
        
        //Only read datasets that are specified (or all if the names are null)
        if(names == null || ArrayUtils.contains(names, ds.getName())) {
          int rank = ds.getRank();
          //Ignore ds that has rank >= 3
          if(rank < 3) {
            Object dso = ds.read();
            if(dso instanceof Vector){
              Vector v = (Vector)dso;
              dso = v.get(0);
            } 
            
            data.put(ds.getName(), dso);
          }
        }
      }
    }
  }
}
