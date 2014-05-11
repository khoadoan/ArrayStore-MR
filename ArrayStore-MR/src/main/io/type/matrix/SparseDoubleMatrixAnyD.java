package io.type.matrix;

import hadoop.io.types.map.SparseOpenLongDoubleHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import array.utils.JArrayUtils;

public class SparseDoubleMatrixAnyD extends DoubleMatrixAnyD {
  protected SparseOpenLongDoubleHashMap elements;
  
  public SparseDoubleMatrixAnyD() {
    
  }

  public SparseDoubleMatrixAnyD(int[] dims) {
    this(dims, null);
  }
  
  public SparseDoubleMatrixAnyD(int[] dims, String[] dimNames) {
    this(dims, dimNames, null);
  }
  
  public SparseDoubleMatrixAnyD(int[] dims, String[] dimNames, int[] dimZeros) {
    this(dims, dimNames, dimZeros,  null, getIntialCapacity(dims), 0.2, 0.5);
  }
  
  public SparseDoubleMatrixAnyD(int[] dims, String[] dimNames, int[] dimZeros, SparseOpenLongDoubleHashMap elements) {
    this(dims, dimNames, dimZeros, null, elements);
  }
  
  public SparseDoubleMatrixAnyD(int[] dims, String[] dimNames, int[] dimZeros, int[] strides, int initialCapacity, double minLoadFactor, double maxLoadFactor) {
    setUp(dims, dimNames, dimZeros, strides);
    this.elements = new SparseOpenLongDoubleHashMap(initialCapacity, minLoadFactor, maxLoadFactor);
  }
  
  public SparseDoubleMatrixAnyD(int[] dims, String[] dimNames, int[] dimZeros, int[] strides, SparseOpenLongDoubleHashMap elements) {
    setUp(dims, dimNames, dimZeros, strides);
    this.elements = elements;
  }
  
  protected void initialize(int rank) {
    dSizes = new int[rank];
    dNames = new String[rank];
    dZeros = new int[rank];
    dStrides = new int[rank];
  }
  
  @Override
  public DoubleMatrixAnyD view(int[] newDims) {
    return new SparseDoubleMatrixAnyD(dSizes, dNames, dZeros, this.elements);
  }

  @Override
  public DoubleMatrixAnyD view(int[] sliceDims, int[] sliceValues) {
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
    
    return new SparseDoubleMatrixAnyD(newDims, JArrayUtils.subarray(dNames, newDimIndices), newDimZeros, newStrides, this.elements);
  }

  @Override
  public DoubleMatrixAnyD view(int[] diceDims, int[][] diceRanges) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setQuick(double value, int... coords) {
    if(value == NULL)
      this.elements.removeKey(getIndex(coords));
    else
      this.elements.put(getIndex(coords), value);
  }

  @Override
  public double getQuick(int... coords) {
    return elements.get(getIndex(coords));
  }

  protected int getIndex(int... coords) {
    int index = 0;
    for(int i=0; i<coords.length; i++) {
      index += dZeros[i] + coords[i]*dStrides[i];
    }
    return index;
  }
  
  public static int getIntialCapacity(int[] dims) {
    int initialCapacity = 1;
    for(int i=0; i < dims.length-1; i++)
      initialCapacity *= dims[i];
    initialCapacity *= dims[dims.length-1]/1000;
    return initialCapacity;
  }
  
  @Override
  public void trimToSize() {
    this.elements.trimToSize();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, dSizes.length);
    for(int i=0; i<dSizes.length; i++)
    {
      WritableUtils.writeVInt(out, dSizes[i]);
      WritableUtils.writeCompressedString(out, dNames[i]);
      WritableUtils.writeVInt(out, dZeros[i]);
      WritableUtils.writeVInt(out, dStrides[i]);
    }
    
    this.elements.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int rank = WritableUtils.readVInt(in);
    initialize(rank);
    
    for(int i=0; i<rank; i++) {
      this.dSizes[i] = WritableUtils.readVInt(in);
      this.dNames[i] = WritableUtils.readCompressedString(in);
      this.dZeros[i] = WritableUtils.readVInt(in);
      this.dStrides[i] = WritableUtils.readVInt(in);
    }
    
    this.elements.readFields(in);
  }

  @Override
  public int compareTo(AbstractMatrixAnyD o) {
    return 0;
  }
}
