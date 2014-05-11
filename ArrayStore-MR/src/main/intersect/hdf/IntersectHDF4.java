package intersect.hdf;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import javax.activation.UnsupportedDataTypeException;

import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import array.Attribute;

public abstract class IntersectHDF4 extends AbstractHDF4 implements Writable{
  protected int length = 0;
  protected FileFormat hdf = null;
  protected Map<String, Attribute> dataFields = null;
  String filename = null;
  
  public IntersectHDF4() {
    this.dataFields = new HashMap<String, Attribute>();
  }
  
  public void open(String filename) {
    try {
      this.filename = filename;
      hdf = openFile4Read(filename);
      Group root = (Group)((javax.swing.tree.DefaultMutableTreeNode)hdf.getRootNode()).getUserObject();
      if(dataFields == null)
        dataFields = new HashMap<String, Attribute>();
      else 
        dataFields.clear();
      
      readData(root);
    } catch(Exception e) {
      throw new RuntimeException("Cannot open file.");
    }
  }
  
  /**
   * Get the value of the 1-D attribute at an index
   * @param name
   * @param index
   * @return
   * @throws UnsupportedDataTypeException
   */
  public double getDouble(String name, int index) throws UnsupportedDataTypeException {
    Object dso = this.dataFields.get(name).getValues();
    
    if(dso instanceof int[]) {
      int[] ds = (int[])dso;
      if(ds.length == 1) {
        return (double)ds[0];
      } else {
        return (double)ds[index];
      }
    } else if(dso instanceof short[]) {
      short[] ds = (short[])dso;
      if(ds.length == 1) {
        return (double)ds[0];
      } else {
        return (double)ds[index];
      }
    } else if(dso instanceof byte[]) {
      byte[] ds = (byte[])dso;
      if(ds.length == 1) {
        return (double)ds[0];
      } else {
        return (double)ds[index];
      }
    } else if(dso instanceof float[]) {
      float[] ds = (float[])dso;
      if(ds.length == 1) {
        return (double)ds[0];
      } else {
        return (double)ds[index];
      }
    } else if(dso instanceof double[]){
      double[] ds = (double[])dso;
      if(ds.length == 1) {
        return ds[0];
      } else {
        return ds[index];
      }
    } else {
      throw new UnsupportedDataTypeException("Unsupported data type conversion: " + dso.getClass().getName());
    }
  }
  
  /**
   * Get the value of the 2-D (swath) attribute of a ray at an index
   * @param name
   * @param index
   * @param ray
   * @return
   * @throws UnsupportedDataTypeException
   */
  public double getDouble(String name, int index, int ray) throws UnsupportedDataTypeException {
    int[] attrDims = this.dataFields.get(name).getCoords();
    Object dso = this.dataFields.get(name).getValues();
    
    int flattenIndex = index * attrDims[1] + ray;
    
    if(dso instanceof int[]) {
      int[] ds = (int[])dso;
      return (double)ds[flattenIndex];
    } else if(dso instanceof short[]) {
      short[] ds = (short[])dso;
      return (double)ds[flattenIndex];
    } else if(dso instanceof byte[]) {
      byte[] ds = (byte[])dso;
      return (double)ds[flattenIndex];
    } else if(dso instanceof float[]) {
      float[] ds = (float[])dso;
//      if(flattenIndex >= ds.length) {
//        throw new RuntimeException(name.toUpperCase() + ": index = " + index + " and ray = " + ray + " and flatten = " + flattenIndex + " while length = " + ds.length);
//      }
      return (double)ds[flattenIndex];
    } else if(dso instanceof double[]){
      double[] ds = (double[])dso;
      return ds[flattenIndex];
    } else {
      throw new UnsupportedDataTypeException("Unsupported data type conversion: " + dso.getClass().getName());
    }
  }
  
  /**
   * Get the average value of the 2-D attribute, where the average is over the columns
   * @param name
   * @param index
   * @return
   * @throws UnsupportedDataTypeException
   */
  public double getAvgDouble(String name, int index) throws UnsupportedDataTypeException {
    int[] attrDims = this.dataFields.get(name).getCoords();
    Object dso = this.dataFields.get(name).getValues();
    
    int startIndex = index * attrDims[1];
    
    double sum = 0;
    if(dso instanceof int[]) {
      int[] ds = (int[])dso;
      for(int i=0; i < attrDims[1]; i++) {
        sum += ds[startIndex+i];
      }
      return sum/attrDims[1];
    } else if(dso instanceof short[]) {
      short[] ds = (short[])dso;
      for(int i=0; i < attrDims[1]; i++) {
        sum += ds[startIndex+i];
      }
      return sum/attrDims[1];
    } else if(dso instanceof byte[]) {
      byte[] ds = (byte[])dso;
      for(int i=0; i < attrDims[1]; i++) {
        sum += ds[startIndex+i];
      }
      return sum/attrDims[1];
    } else if(dso instanceof float[]) {
      float[] ds = (float[])dso;
      for(int i=0; i < attrDims[1]; i++) {
        sum += ds[startIndex+i];
      }
      return sum/attrDims[1];
    } else if(dso instanceof double[]){
      double[] ds = (double[])dso;
      for(int i=0; i < attrDims[1]; i++) {
        sum += ds[startIndex+i];
      }
      return sum/attrDims[1];
    } else {
      throw new UnsupportedDataTypeException("Unsupported data type conversion: " + dso.getClass().getName());
    }
  }
  
  public double getInt(String name, int index) throws UnsupportedDataTypeException {
    Object data = this.dataFields.get(name);
    
    if(data instanceof int[]) {
      return ((int[]) data)[index];
    } else if(data instanceof short[]) {
      return (int) ((short[]) data)[index];
    } else if(data instanceof byte[]) {
      return (int) ((byte[]) data)[index];
    } else {
      throw new UnsupportedDataTypeException("Unsupported data type conversion: " + data.getClass().getName());
    }
  }
  
  
  protected abstract void readData(Group root); 
  public abstract boolean next();
  public abstract long[] getCoordinates();
  public abstract double[] getAttributes();
  public abstract double[] getRecord();
  public int getLength() {
    return this.length;
  }
  
  protected void readData(DataInput in) throws IOException {
    if(this.dataFields == null) {
      this.dataFields = new HashMap<String, Attribute>();
    } else {
      this.dataFields.clear();
    }
    
    int numFields = WritableUtils.readVInt(in);
    for(int i=0; i<numFields; i++) {
      String name = WritableUtils.readCompressedString(in);
      int[] attrDims = new int[WritableUtils.readVInt(in)];
      for(int j=0; j<attrDims.length; j++) {
        attrDims[j] = WritableUtils.readVInt(in);
      }
      readFields(in, name, attrDims);
    }
  }
  
  protected void writeData(DataOutput out) throws IOException {
    //Write number of fields
    WritableUtils.writeVInt(out, this.dataFields.size());
    for(String name: this.dataFields.keySet()) {
      Attribute attr = this.dataFields.get(name);
      //Write attribute name
      WritableUtils.writeCompressedString(out, name);
      int[] attrDims = attr.getCoords();
      //Write attribute dims
      WritableUtils.writeVInt(out, attrDims.length);
      for(int i=0; i<attrDims.length; i++) { 
        WritableUtils.writeVInt(out, attrDims[i]);
      }
      //Write attribute's values
      write(out, attr.getValues());
    }
  }
  
  public void write(DataOutput out, Object value) throws IOException {
    if(value instanceof byte[]) {
      out.writeByte(TBYTE);
      WritableUtils.writeCompressedByteArray(out, (byte[]) value);
    } else if(value instanceof short[]) {
      out.writeByte(TSHORT);
      short[] d = (short[]) value;
      WritableUtils.writeVInt(out, d.length);
      for(int i=0; i<d.length; i++)
        out.writeShort(d[i]);
    } else if(value instanceof int[]) {
      out.writeByte(TINT);
      short[] d = (short[]) value;
      WritableUtils.writeVInt(out, d.length);
      for(int i=0; i<d.length; i++)
        WritableUtils.writeVInt(out, d[i]);
    } else if(value instanceof float[]) {
      out.writeByte(TFLOAT);
      float[] d = (float[]) value;
      WritableUtils.writeVInt(out, d.length);
      for(int i=0; i<d.length; i++)
        out.writeFloat(d[i]);
    } else if(value instanceof double[]) {
      out.writeByte(TDOUBLE);
      double[] d = (double[]) value;
      WritableUtils.writeVInt(out, d.length);
      for(int i=0; i<d.length; i++)
        out.writeDouble(d[i]);
    }
  }
  
  public void readFields(DataInput in, String name, int[] attrDims) throws IOException {
    byte type = in.readByte();
    switch(type) {
      case TBYTE:
        this.dataFields.put(name, new Attribute(attrDims, WritableUtils.readCompressedByteArray(in)));
        break;
      case TSHORT:
        {
          short[] d = new short[WritableUtils.readVInt(in)];
          for(int i=0; i<d.length; i++) {
            d[i] = in.readShort();
          }
          this.dataFields.put(name, new Attribute(attrDims, d));
        }
        break;
      case TINT:
        {
          int[] d = new int[WritableUtils.readVInt(in)];
          for(int i=0; i<d.length; i++) {
            d[i] = WritableUtils.readVInt(in);
          }
          this.dataFields.put(name, new Attribute(attrDims, d));
        }
        break;
      case TFLOAT:
        {
          float[] d = new float[WritableUtils.readVInt(in)];
          for(int i=0; i<d.length; i++) {
            d[i] = in.readFloat();
          }
          this.dataFields.put(name, new Attribute(attrDims, d));
        }
        break;
      case TDOUBLE:
        {
          double[] d = new double[WritableUtils.readVInt(in)];
          for(int i=0; i<d.length; i++) {
            d[i] = in.readDouble();
          }
          this.dataFields.put(name, new Attribute(attrDims, d));
        }
        break;
      default:
    }
  }
  
  public void close() {
    if(hdf != null) {
      try {
        hdf.close();
      } catch (Exception e) {
        throw new RuntimeException("cannot close hdf file", e);
      }
    }
  }
  
  public static IntersectHDF4 getReader(String className) {
    IntersectHDF4 reader = null;
    try {
      reader = (IntersectHDF4) Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Cannot create reader " + className, e);
    }
    
    return reader;
  }
  
  private static final int MS_IN_HOUR = 60*60*1000;
  private static final int MS_IN_MINUTE = 60*1000;
  public static long getTimestamp(int year, int doy, double ms) {
    int hh = (int) Math.floor(ms / MS_IN_HOUR);
    int mm = (int) Math.floor((ms % MS_IN_HOUR) / MS_IN_MINUTE);
    int ss = (int) Math.floor((ms % MS_IN_MINUTE) / 1000);
    int SSS = (int) (ms % 1000);
    
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    c.set(Calendar.YEAR, year);
    c.set(Calendar.DAY_OF_YEAR, doy);
    c.set(Calendar.HOUR_OF_DAY, hh);
    c.set(Calendar.MINUTE, mm);
    c.set(Calendar.SECOND, ss);
    c.set(Calendar.MILLISECOND, SSS);
    
    return c.getTimeInMillis();
  }
}
