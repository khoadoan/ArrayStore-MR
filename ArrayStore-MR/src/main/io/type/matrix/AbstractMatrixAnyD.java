package io.type.matrix;

import org.apache.hadoop.io.WritableComparable;

import cern.colt.matrix.AbstractMatrix;

public abstract class AbstractMatrixAnyD extends AbstractMatrix implements WritableComparable<AbstractMatrixAnyD>{
  protected int[] dSizes;
  protected String[] dNames;
  protected int[] dStrides;
  protected int[] dZeros;
  
  @Override
  public long size() {
    long size = 1;
    for(int i=1; i<dSizes.length; i++) {
      size *= dSizes[i];
    }
    return size;
  }
  
  protected void setUp(int[] dims, String[] dimNames) {
    setUp(dims, dimNames, null, null);
  }
  
  protected void setUp(int[] dims, String[] dimNames, int[] dimZeros, int[] strides) {
    //Check argument
    if((dimNames != null && dimNames.length != dims.length) || 
        (dimZeros != null && dimZeros.length != dims.length) || (strides != null && strides.length != dims.length))
      throw new IllegalArgumentException("Dimension info must have matching length");
    
    //Check positive dims
    for(int i=0; i<dims.length; i++) {
      if(dims[i] < 0)
        throw new IllegalArgumentException("negative size");
    }
    
    double size = 1;
    for(int i=0; i<dims.length; i++) {
      size *= dims[i];
    }
    
    //Support 32-bit Integer matrix size
    if(size > Integer.MAX_VALUE)
      throw new IllegalArgumentException("matrix too large");
    
    
    this.dSizes = new int[dims.length];
    this.dStrides = new int[dims.length];
    this.dZeros = new int[dims.length];
    for(int i=0; i<dims.length; i++) {
      this.dSizes[i] = dims[i];
      
      if(dimZeros != null) {
        this.dZeros[i] = dimZeros[i];
      } else {
        this.dZeros[i] = 0;
      }
      
      if(strides != null) {
        this.dStrides[i] = strides[i];
      } else {
        int numOfElements = 1;
        for(int j=i+1; j < dims.length; j++) {
          numOfElements *= dims[j];
        }
        this.dStrides[i] = numOfElements;
      }
    }

    // Support lookup dimension by name
    this.dNames = new String[dims.length];
    if(dimNames != null) {
      for(int i=0; i<dimNames.length; i++) {
        this.dNames[i] = dimNames[i];
      }
    } else {
      for(int i=0; i<this.dNames.length; i++) {
        this.dNames[i] = String.valueOf(i);
      }
    }
  }
  
  protected void checkSlice(int[] sliceDims, int[] sliceValues) {
    if(sliceDims.length != sliceValues.length) {
      throw new RuntimeException("Slice dimensions must have corresponding values");
    }
    
    for(int i=0; i<sliceDims.length; i++) {
      if(sliceValues[i] < 0 || sliceValues[i] > dSizes[sliceDims[i]])
        throw new IndexOutOfBoundsException("Attempted to access " + sliceValues[i]  + " of dimension " + sliceDims[i]);
    }
  }
  
  protected void checkDice(int[] diceDims, int[][] diceRanges) {
    if(diceDims.length != diceRanges.length)
    {    
      throw new RuntimeException("Each dimension of dicing must have a range");
    }
    
    for(int i=0; i<diceDims.length; i=i++) {
      int dimStart = diceRanges[i][0];
      int dimEnd = diceRanges[i][1];
      
      if(dimStart < dimEnd || dimEnd < 0 || dimStart < 0 || dimStart > dSizes[diceDims[i]] || dimEnd > dSizes[diceDims[i]]) {
        throw new IndexOutOfBoundsException("Attempted to access range [" 
              + dimStart  + ", " + dimEnd + "] of dimension " + diceDims[i]);
      }
    }
  }
}
