package array;

import org.apache.commons.lang.ArrayUtils;

import array.utils.JArrayUtils;

public abstract class AbstractArrayAnyD implements java.io.Serializable, Cloneable{
  private static final long serialVersionUID = 1L;
  
  public static final byte TYPE_INT32 = 1;
  public static final byte TYPE_INT64 = 2;
  public static final byte TYPE_DOUBLE = 3; //Default type
  
  
  //Array Metadata
  protected int[] dSizes;
  protected String[] dNames;
  protected int[] dStrides;
  protected int[] dZeros; //d - dimension
  protected String[] caNames; //ca - Cell Attribute
  protected byte[] caTypes;

  public AbstractArrayAnyD() {}

  protected void initialize(int rank, int numOfAttributes, boolean hasDNames, boolean hasCANames) {
    dSizes = new int[rank];
    if(hasDNames)
      dNames = new String[rank];
    dZeros = new int[rank];
    dStrides = new int[rank];

    //Do not intialize the attribute store if the length is 0
    if(numOfAttributes > 0) {
      if(hasCANames)
        caNames = new String[numOfAttributes];
      caTypes = new byte[numOfAttributes];
    }
  }
  
  protected void setUp(int[] dSizes, String[] dNames, String[] caNames, byte[] caTypes) {
    setUp(dSizes, dNames, caNames, caTypes, null, null);
  }
  
  protected void setUp(int[] dSizes, String[] dNames, String[] caNames, byte[] caTypes, int[] dZeros, int[] dStrides) {
    //Check argument
    if((dNames != null && dNames.length != dSizes.length) || 
        (dZeros != null && dZeros.length != dSizes.length) || (dStrides != null && dStrides.length != dSizes.length))
      throw new IllegalArgumentException("Dimension info must have matching length."); 
        //+ (dSizes == null ? "?" : dSizes.length) + " " 
        //+ (dNames == null ? "?" : dNames[0]) + " " + (dZeros == null ? "?" : dZeros.length) + " " + (dStrides == null ? "?" : dStrides.length));
    
    if(caNames == null || (caTypes != null && caNames.length != caTypes.length)) 
      throw new IllegalArgumentException("Invalid Attribute information");
    
    //Check positive dims
    for(int i=0; i<dSizes.length; i++) {
      if(dSizes[i] < 0)
        throw new IllegalArgumentException("negative size");
    }
    
    double size = 1;
    for(int i=0; i<dSizes.length; i++) {
      size *= dSizes[i];
    }
    
    //Support 32-bit Integer matrix size
    if(size > Long.MAX_VALUE)
      throw new IllegalArgumentException("matrix too large " + size + ": " + JArrayUtils.toString(dSizes, null));
    
    this.dSizes = new int[dSizes.length];
    this.dStrides = new int[dSizes.length];
    this.dZeros = new int[dSizes.length];
    if(dNames != null)
      this.dNames = new String[dSizes.length];

    //Setup the dimensions
    for(int i=0; i<dSizes.length; i++) {
      this.dSizes[i] = dSizes[i];
      
      if(dZeros != null) {
        this.dZeros[i] = dZeros[i];
      } else {
        this.dZeros[i] = 0;
      }
      
      //TODO: change to start from length
      if(dStrides != null) {
        this.dStrides[i] = dStrides[i];
      } else {
        int numOfElements = 1;
        for(int j=i+1; j < dSizes.length; j++) {
          numOfElements *= dSizes[j];
        }
        this.dStrides[i] = numOfElements;
      }
      
      if(dNames != null) {
        this.dNames[i] = dNames[i];
      }
    }
    
    //Setup the attributes
    if(caNames != null)
      this.caNames = new String[caNames.length];
    this.caTypes = new byte[caNames.length];
    for(int i=0; i<caNames.length; i++) {
      if(caNames != null)
        this.caNames[i] = caNames[i];
      
      if(caTypes != null) {
        this.caTypes[i] = caTypes[i];
      } else {
        this.caTypes[i] = TYPE_DOUBLE;
      }
    }
  }
  
  public byte getAttributeType(String caName) {
    return this.getAttributeType(ArrayUtils.indexOf(this.caNames, caName));
  }
  
  public byte getAttributeType(int index) {
    return this.caTypes[index];
  }
  
  public void setDouble(String name, double value, int... coords) {
    setDouble(ArrayUtils.indexOf(this.dNames, name), value, coords);
  }
  
  public void setDouble(int index, double value, int... coords) {
    if(coords.length != dSizes.length) {
      throw new RuntimeException("Number of dimensions must match.");
    }
    
    setQuickDouble(index, value, coords);
  }
  
  protected abstract void setQuickDouble(int index, double value, int... coords);
  
  public double getDouble(String name, int... coords) {
    return getDouble(ArrayUtils.indexOf(this.dNames, name), coords);
  }
  
  public double getDouble(int index, int... coords) {
    if(coords.length != dSizes.length) {
      throw new RuntimeException("Number of dimensions must match.");
    }
    
    return getQuickDouble(index, coords);
  }
  
  protected abstract double getQuickDouble(int index, int... coords);
  
  /**
   * Check if the information of slicing is legal.
   * @param sliceDims
   * @param sliceValues
   */
  protected void checkSlice(int[] sliceDims, int[] sliceValues) {
    if(sliceDims.length != sliceValues.length) {
      throw new RuntimeException("Slice dimensions must have corresponding values");
    }
    
    for(int i=0; i<sliceDims.length; i++) {
      if(sliceValues[i] < 0 || sliceValues[i] > dSizes[sliceDims[i]])
        throw new IndexOutOfBoundsException("Attempted to access " + sliceValues[i]  + " of dimension " + sliceDims[i]);
    }
  }
  
  /**
   * Check if the information of dicing is legal.
   * @param diceDims
   * @param diceRanges
   */
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
  
  /**
   * Compute the total size of the array, including empty cells.
   * @return
   */
  public long size() {
    return 0;
  }
  
  /**
   * Optimize data store of this array.
   */
  public void trimToSize() {}
  
  public abstract <T extends AbstractArrayAnyD> T slice(int[] sliceDims, int[] sliceValues);
  public abstract <T extends AbstractArrayAnyD> T dice(int[] diceDims, int[][] diceRanges);
  
  /**
   * Slice the array, but the slice references the actual data of this array.
   * Any changes made on the slice will also affect the this array's data.
   * @param sliceDims
   * @param sliceValues
   * @return
   */
  public AbstractArrayAnyD viewSlice(int[] sliceDims, int[] sliceValues) {
    checkSlice(sliceDims, sliceValues);
    
    return view(sliceDims, sliceValues);
  }
  
  /**
   * Dice the array, but the dice references the actual data of this array.
   * Any changes made on the dice will also affect the this array's data.
   * @param diceDims
   * @param diceRanges
   * @return
   */
  public AbstractArrayAnyD viewDice(int[] diceDims, int[][] diceRanges) {
    checkDice(diceDims, diceRanges);
    
    return view(diceDims, diceRanges);
  } 
  
  public abstract AbstractArrayAnyD view(int[] sliceDims, int[] sliceValues);
  
  public abstract AbstractArrayAnyD view(int[] diceDims, int[][] diceRanges);
}
