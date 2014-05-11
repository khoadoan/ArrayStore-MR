package hadoop.io.types.array;

import org.apache.hadoop.io.WritableUtils;



/**
 * @author Khoa
 * A wrapper around chunk's data, containing separate D-dimensional matrix 
 * for each attribute.
 * Only support double attributes at the moment.
 */
public class ChunkDoubleDataWritable extends SparseDoubleArrayAnyD {
  protected static final byte TYPE_INT32 = 1;
  protected static final byte TYPE_INT64 = 2;
  protected static final byte TYPE_FLOAT = 3;
  protected static final byte TYPE_DOUBLE = 4;
  
  public ChunkDoubleDataWritable() {
    
  }
  
  public ChunkDoubleDataWritable(int[] dSizes, String[] dNames, String[] caNames) {
    super(dSizes, dNames, caNames, null, null, DEFAULT_CAPACITY, 0.2, 0.5);
  }
  
  public void setDouble(double[] values, int[] coords) {
    if(coords.length != dSizes.length) {
      throw new RuntimeException("Number of dimensions must match.");
    }
    
    for(int i=0; i<values.length; i++) {
      setQuickDouble(i, values[i], coords);
    }
  }
  
  public double[] getDouble(int[] coords) {
    if(coords.length != dSizes.length) {
      throw new RuntimeException("Number of dimensions must match.");
    }
    
    double[] attributeValues = new double[this.caTypes.length];
    for(int i=0; i<this.elements.length; i++) {
      attributeValues[i] = getQuickDouble(i, coords);
    }
    
    return attributeValues;
  }
  
//  public int getChunkPhysicalSize() {
//    int physicalSize = 0;
//    physicalSize += WritableUtils.getVIntSize(dSizes.length);
//    physicalSize += WritableUtils.getVIntSize(caTypes.length);
//    physicalSize += 2;
//    for(int i=0; i<dSizes.length; i++)
//    {
//      physicalSize += WritableUtils.getVIntSize(dSizes[i]);
//      if(dNames != null) {
//        physicalSize += WritableUtils.writeCompressedStringArray(arg0, arg1)
//      }
//      WritableUtils.writeVInt(out, dZeros[i]);
//      WritableUtils.writeVInt(out, dStrides[i]);
//    }
//    
//    //Write Attribute Info
//    if(caNames != null) {
//      WritableUtils.writeCompressedStringArray(out, caNames);
//    }
//    WritableUtils.writeCompressedByteArray(out, caTypes);
//    
//    //Write actual data of the cube
//    for(int i=0; i<elements.length; i++) {
//      this.elements[i].write(out);
//    }
//    return physicalSize;
//  }
}
