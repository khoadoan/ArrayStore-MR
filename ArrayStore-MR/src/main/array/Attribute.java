package array;

import array.utils.JArrayUtils;

public class Attribute {
  protected int[] coords;
  protected Object values;
  
  public Attribute() {
    // TODO Auto-generated constructor stub
  }
  
  public Attribute(int[] dims, Object values) {
    set(dims, values);
  }
  
  public Attribute(long[] coords, Object values) {
    set(JArrayUtils.toInts(coords), values);
  }
  
  public void set(int[] coords, Object values) {
    this.coords = coords;
    this.values = values;
  }
  
  public int[] getCoords() {
    return this.coords;
  }
  
  public Object getValues() {
    return values;
  }
}
