package io.type.matrix;


public abstract class DoubleMatrixAnyD extends AbstractMatrixAnyD {
  public static final double NULL = Double.NaN;
  
  public DoubleMatrixAnyD() {}
  
  public DoubleMatrixAnyD view() {
    return view(this.dSizes);
  }
  
  public abstract DoubleMatrixAnyD view(int[] newDims);
  
  public abstract DoubleMatrixAnyD view(int[] sliceDims, int[] sliceValues);
  
  public abstract DoubleMatrixAnyD view(int[] diceDims, int[][] diceRanges);
  
  public void set(double value, int... coords) {
    if(coords.length != dSizes.length) {
      throw new RuntimeException("Number of dimensions must match.");
    }
    
    setQuick(value, coords);
  }
  
  public abstract void setQuick(double value, int... coords);
  
  public double get(int... coords) {
    if(coords.length != dSizes.length) {
      throw new RuntimeException("Number of dimensions must match.");
    }
    
    return getQuick(coords);
  }
  
  public abstract double getQuick(int... coords);
  
  public DoubleMatrixAnyD viewSlice(int[] sliceDims, int[] sliceValues) {
    checkSlice(sliceDims, sliceValues);
    
    return view(sliceDims, sliceValues);
  }
  
  public DoubleMatrixAnyD viewDice(int[] diceDims, int[][] diceRanges) {
    checkDice(diceDims, diceRanges);
    
    return view(diceDims, diceRanges);
  }
  
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    if(dSizes.length == 4) {
      //Only used to test 4x4 matrix
      for(int i=0; i < dSizes[0]; i++) {
        for(int j=0; j < dSizes[1]; j++) {
          for(int k=0; k < dSizes[2]; k++) {
            for(int l=0; l < dSizes[3]; l++) {
              s.append(get(i, j, k, l)).append("\t");
            }
            s.append("\n");
          }
          s.append("--------\n");
        }
        s.append("*********\n*********\n");
      }
    } else if(dSizes.length == 3) {
    //Only used to test 4x4 matrix
      for(int i=0; i < dSizes[0]; i++) {
        for(int j=0; j < dSizes[1]; j++) {
          for(int k=0; k < dSizes[2]; k++) {
            s.append(get(i, j, k)).append("\t");
          }
          s.append("--------\n");
        }
        s.append("*********\n*********\n");
      }
    }
    else if(dSizes.length == 2) {
      //Only used to test 4x4 matrix
        for(int i=0; i < dSizes[0]; i++) {
          for(int j=0; j < dSizes[1]; j++) {
            s.append(get(i, j)).append("\t");
          }
          s.append("\n--------\n");
        }
      }
    return s.toString();
  }
}
