package hadoop.io.types.array;

import hadoop.io.types.map.SparseOpenLongDoubleHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import array.AbstractArrayAnyD;
import array.utils.JArrayUtils;

public class SparseDoubleArrayAnyD extends AbstractArrayAnyD implements Writable{
  public static final int DEFAULT_CAPACITY = 10000;
  public static final double NULL = Double.NaN;
  
  protected SparseOpenLongDoubleHashMap[] elements;
  
  public SparseDoubleArrayAnyD() {
    
  }
  
  public SparseDoubleArrayAnyD(int[] dSizes, String[] dNames, String[] caNames, int[] dZeros, int[] dStrides) {
    this(dSizes, dNames, caNames, dZeros, dStrides, DEFAULT_CAPACITY, 0.2, 0.5);
  }
  
  public SparseDoubleArrayAnyD(int[] dSizes, String[] dNames, String[] caNames, int[] dZeros, int[] dStrides, int initialCapacity, double minLoadFactor, double maxLoadFactor) {
    setUp(dSizes, dNames, caNames, null, dZeros, dStrides);
    this.elements = new SparseOpenLongDoubleHashMap[caNames.length];
    for(int i=0; i<caNames.length; i++) {
      this.elements[i] = new SparseOpenLongDoubleHashMap(initialCapacity, minLoadFactor, maxLoadFactor);
    }
  }
  
  public SparseDoubleArrayAnyD(int[] dSizes, String[] dNames, String[] caNames, int[] dZeros, int[] dStrides, SparseOpenLongDoubleHashMap[] elements) {
    setUp(dSizes, dNames, caNames, null, dZeros, dStrides);
    this.elements = elements;
  }
  
  @Override
  public AbstractArrayAnyD view(int[] sliceDims, int[] sliceValues) {
    // Slice Dims must be in the same order of the original array
    int newRank = dSizes.length-sliceDims.length;
    int[] newStrides = new int[newRank];
    int[] newDimZeros = new int[newRank];
    int[] newDims = new int[newRank];
    int[] newDimIndices = new int[newRank];
    
    /* Keep the same indices of the elements, but change the lookup so that the new sub-matrix 
     * lookup gives the same location.
     * i: iterate over this array's dims
     * j: iterate over the slices
     * k: iterate over the new subarray's dims
     */
    for(int i=0, j=0, k = 0, s = 0; i<dSizes.length; i++) {
      if(j < sliceDims.length && i == sliceDims[j]) {
        s += dStrides[i] * sliceValues[j];
        j++;
      } else {
        newDimZeros[k] = s;
        newStrides[k] = dStrides[i];
        newDims[k] = dSizes[i];
        newDimIndices[k] = i;
        s = 0;
        k++;
      }
    }
    return new SparseDoubleArrayAnyD(newDims, JArrayUtils.subarray(dNames, newDimIndices), this.caNames, newDimZeros, newStrides, this.elements);
  }

  @Override
  public AbstractArrayAnyD view(int[] diceDims, int[][] diceRanges) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends AbstractArrayAnyD> T dice(int[] diceDims, int[][] diceRanges) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void setQuickDouble(int index, double value, int... coords) {
    if(value == NULL) {
      this.elements[index].removeKey(getIndex(coords));
    } else {
      this.elements[index].put(getIndex(coords), value);
    }
  }

  @Override
  protected double getQuickDouble(int index, int... coords) {
    return this.elements[index].get(getIndex(coords));
  }
  
  protected int getIndex(int... coords) {
    int index = 0;
    for(int i=0; i<coords.length; i++) {
      index += dZeros[i] + coords[i]*dStrides[i];
    }
    return index;
  }
  
  @Override
  public <T extends AbstractArrayAnyD> T slice(int[] sliceDims, int[] sliceValues) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    //Write Dimension Info
    WritableUtils.writeVInt(out, dSizes.length);
    WritableUtils.writeVInt(out, caTypes.length);
    out.writeBoolean(dNames != null);
    out.writeBoolean(caNames != null);
    for(int i=0; i<dSizes.length; i++)
    {
      WritableUtils.writeVInt(out, dSizes[i]);
      if(dNames != null)
        WritableUtils.writeCompressedString(out, dNames[i]);
      WritableUtils.writeVInt(out, dZeros[i]);
      WritableUtils.writeVInt(out, dStrides[i]);
    }
    
    //Write Attribute Info
    if(caNames != null) {
      WritableUtils.writeCompressedStringArray(out, caNames);
    }
    WritableUtils.writeCompressedByteArray(out, caTypes);
    
    //Write actual data of the cube
    for(int i=0; i<elements.length; i++) {
      this.elements[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int rank = WritableUtils.readVInt(in);
    int numOfAttributes = WritableUtils.readVInt(in);
    boolean hasDNames = in.readBoolean();
    boolean hasCANames = in.readBoolean();
    initialize(rank, numOfAttributes, hasDNames, hasCANames);
    
    for(int i=0; i<rank; i++) {
      this.dSizes[i] = WritableUtils.readVInt(in);
      if(hasDNames)
        this.dNames[i] = WritableUtils.readCompressedString(in);
      this.dZeros[i] = WritableUtils.readVInt(in);
      this.dStrides[i] = WritableUtils.readVInt(in);
    }

    if(hasCANames)
      this.caNames = WritableUtils.readCompressedStringArray(in);
    this.caTypes = WritableUtils.readCompressedByteArray(in);
    
    this.elements = new SparseOpenLongDoubleHashMap[this.caNames.length];
    for(int i=0; i<this.caTypes.length; i++) {
      SparseOpenLongDoubleHashMap element = new SparseOpenLongDoubleHashMap();
      element.readFields(in);
      this.elements[i] = element;
    }
  }
}
