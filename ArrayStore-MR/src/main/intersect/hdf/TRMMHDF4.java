package intersect.hdf;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.activation.UnsupportedDataTypeException;

import ncsa.hdf.object.Group;

import org.apache.hadoop.io.WritableUtils;

import array.Attribute;

public class TRMMHDF4 extends IntersectHDF4{
  public static final int MIDDLE_RAY = 24;
  protected int[] dims = null;
  protected int currentIndex = -1;
  protected long[] currentKey = new long[5];
  protected double[] currentValue = new double[10];
  protected double[] currentRecord = new double[17];
  protected Calendar time = null;
  protected int orbitNo;
  
//  public static String[] SWATH_FIELDS = null;
  
  static {
//    String swath_fields_str = "scanTime_sec,Latitude,Longitude,systemNoise,sysNoiseWarnFlag,minEchoFlag,binEllipsoid,binDIDHmean,scLocalZenith,scRange,landOceanFlag,surfWarnFlag,binSurfPeak";
//    SWATH_FIELDS = swath_fields_str.split(",");
  }
  
  public TRMMHDF4() {
    super();
  }
  
  /**
  * Parse TRMM filename to get the calendar object
  * indicating the start of the day
  * 1C21.20070101.52013.7.HDF
  * @param filename
  * @return
  */
  public void parseFilename(String filename) {
    Pattern p = Pattern.compile("1C21\\.(\\d\\d\\d\\d)(\\d\\d)(\\d\\d)\\.(\\d\\d\\d\\d\\d).+");
    Matcher m = p.matcher(filename);
    if(!m.matches()) {
      throw new RuntimeException("Unexpected filename pattern: " + filename);
    }
    
    time = Calendar.getInstance(TimeZone.getTimeZone("GMT")); 
    time.set(Calendar.YEAR, Integer.parseInt(m.group(1)));
    time.set(Calendar.MONTH, Integer.parseInt(m.group(2)) - 1); //First Month is 0
    time.set(Calendar.DATE, Integer.parseInt(m.group(3)));
    time.set(Calendar.HOUR_OF_DAY, 0);
    time.set(Calendar.MINUTE, 0);
    time.set(Calendar.SECOND, 0);
    time.set(Calendar.MILLISECOND, 0);
    
    this.orbitNo = Integer.parseInt(m.group(4));
  }

  @Override
  public void open(String filename) {
    super.open(filename);
    parseFilename(hdf.getName());
  }
  
  @Override
  protected void readData(Group root) {
    try {
      readGroupDatasets(getGroup(root, "pr_cal_coef"), this.dataFields, null);
      readGroupDatasets(getGroup(root, "ray_header"), this.dataFields, null);

      readGroupDatasets(getGroup(root, "Swath"), this.dataFields, null);
      readGroupDatasets(getGroup(root, "Swath", "ScanTime"), this.dataFields, null);
      readGroupDatasets(getGroup(root, "Swath", "scanStatus"), this.dataFields, null);
      readGroupDatasets(getGroup(root, "Swath", "navigation"), this.dataFields, null);
      readGroupDatasets(getGroup(root, "Swath", "powers"), this.dataFields, null);
      
      this.dims = getDims(getGroup(root, "Swath"), "Latitude");
      //Default length to the number of scans
      this.length = this.dims[0];
    } catch (OutOfMemoryError e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public boolean next() {
    if(currentIndex < length-1) {
      currentIndex++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public long[] getCoordinates() {
    try {
      //Return the middle ray's data
      this.currentKey[0] = IntersectUtils.scaleLatLon(getDouble("Latitude", currentIndex, MIDDLE_RAY));
      this.currentKey[1] = IntersectUtils.scaleLatLon(getDouble("Longitude", currentIndex, MIDDLE_RAY));
      this.currentKey[2] = (int)getDouble("Year", currentIndex);
      this.currentKey[3] = (int)getDouble("DayOfYear", currentIndex);
      this.currentKey[4] = IntersectUtils.scaleTime(getDouble("scanTime_sec", currentIndex));
    } catch(Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
    return this.currentKey;
  }

  @Override
  public double[] getAttributes() {
      try {
        this.currentValue[0] = getDouble("systemNoise", currentIndex, MIDDLE_RAY);
        this.currentValue[1] = getDouble("sysNoiseWarnFlag", currentIndex, MIDDLE_RAY);
        this.currentValue[2] = getDouble("minEchoFlag", currentIndex, MIDDLE_RAY);
        this.currentValue[3] = getDouble("binEllipsoid", currentIndex, MIDDLE_RAY);
        this.currentValue[4] = getDouble("binDIDHmean", currentIndex, MIDDLE_RAY);
        this.currentValue[5] = getDouble("scLocalZenith", currentIndex, MIDDLE_RAY);
        this.currentValue[6] = getDouble("scRange", currentIndex, MIDDLE_RAY);
        this.currentValue[7] = getDouble("landOceanFlag", currentIndex, MIDDLE_RAY);
        this.currentValue[8] = getDouble("surfWarnFlag", currentIndex, MIDDLE_RAY);
        this.currentValue[9] = getDouble("binSurfPeak", currentIndex, MIDDLE_RAY);
      } catch (UnsupportedDataTypeException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      return this.currentValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    //Write Filename
    WritableUtils.writeCompressedString(out, this.filename);
    
    //Write Dimension of the swath
    WritableUtils.writeVInt(out, dims.length);
    for(int i=0; i<dims.length; i++) {
      WritableUtils.writeVInt(out, dims[i]);
    }
    
    //Write all the data fields
    writeData(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.filename = WritableUtils.readCompressedString(in);
    parseFilename(filename);
    
    dims = new int[WritableUtils.readVInt(in)];
    for(int i=0; i<dims.length; i++) {
      dims[i] = WritableUtils.readVInt(in);
    }
    this.length = dims[0];
    this.currentIndex = -1;
    
    readData(in);
  }
    
  @Override
  public double[] getRecord() {
    try {
      
      this.currentRecord[0] = getDouble("Latitude", currentIndex, MIDDLE_RAY);
      this.currentRecord[1] = getDouble("Longitude", currentIndex, MIDDLE_RAY);
      this.currentRecord[2] = getDouble("Year", currentIndex);
      this.currentRecord[3] = getDouble("DayOfYear", currentIndex);
      this.currentRecord[4] = IntersectUtils.scaleTime(getDouble("scanTime_sec", currentIndex));//getDouble("scanTime_sec", currentIndex);
      this.currentRecord[5] = orbitNo;

      this.currentRecord[6] = getDouble("systemNoise", currentIndex, MIDDLE_RAY);
      this.currentRecord[7] = getDouble("sysNoiseWarnFlag", currentIndex, MIDDLE_RAY);
      this.currentRecord[8] = getDouble("minEchoFlag", currentIndex, MIDDLE_RAY);
      this.currentRecord[9] = getDouble("binEllipsoid", currentIndex, MIDDLE_RAY);
      this.currentRecord[10] = getDouble("binDIDHmean", currentIndex, MIDDLE_RAY);
      this.currentRecord[11] = getDouble("scLocalZenith", currentIndex, MIDDLE_RAY);
      this.currentRecord[12] = getDouble("scRange", currentIndex, MIDDLE_RAY);
      this.currentRecord[13] = getDouble("landOceanFlag", currentIndex, MIDDLE_RAY);
      this.currentRecord[14] = getDouble("surfWarnFlag", currentIndex, MIDDLE_RAY);
      this.currentRecord[15] = getDouble("binSurfPeak", currentIndex, MIDDLE_RAY);
      this.currentRecord[16] = (double) getTimestamp((int)getDouble("Year", currentIndex), (int)getDouble("DayOfYear", currentIndex), this.currentRecord[4]);
      
    } catch (UnsupportedDataTypeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return this.currentRecord;
  }
  
  @Override
  public String toString() {
    if(this.dataFields != null && this.dataFields.size() > 0) {
      StringBuilder keys = null;
      for(String key: this.dataFields.keySet()) {
        if(keys == null)
          keys = new StringBuilder(key);
        else
          keys.append(", ").append(key);
      }
      return keys.toString();
    } else {
      return String.valueOf(this.dataFields == null);
    }
  }
}
