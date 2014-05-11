package intersect.hdf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.activation.UnsupportedDataTypeException;

import ncsa.hdf.object.Group;

import org.apache.hadoop.io.WritableUtils;

public class CloudSatHDF4 extends IntersectHDF4 {
  protected int currentIndex = -1;
  protected long[] currentKey = new long[5];
  protected double[] currentValue = new double[7];
  protected double[] currentRecord = new double[14];
  
  protected Calendar time = null;
  protected int orbitNo;
  
  
//  public static String[] GEOLOCATION_FIELDS = null;
//  public static String[] DATA_FIELDS = null;

  static {
//    String geolocation_fields_str = "Profile_time,UTC_start,TAI_start,Latitude,Longitude,Height,Range_to_intercept,DEM_elevation,Vertical_binsize,Pitch_offset,Roll_offset";
//    String data_fields_str = "Data_quality,Data_status,Data_targetID,SurfaceHeightBin,SurfaceHeightBin_fraction,CPR_Cloud_mask,Gaseous_Attenuation,Radar_Reflectivity,Sigma-Zero,MODIS_cloud_flag,MODIS_Cloud_Fraction,MODIS_scene_char,MODIS_scene_var,CPR_Echo_Top,sem_NoiseFloor,sem_NoiseFloorVar,sem_NoiseGate,Navigation_land_sea_flag,Clutter_reduction_flag";
//    if(GEOLOCATION_FIELDS == null)
//      GEOLOCATION_FIELDS = geolocation_fields_str.split(",");
//    if(DATA_FIELDS == null)
//      DATA_FIELDS = data_fields_str.split(",");
  }
  
  public CloudSatHDF4() {
    super();
  }
  
  /**
   * Parse CloudSat filename to get the calendar object
   * indicating the start of the day
   * 2007001005141_03607_CS_2B-GEOPROF_GRANULE_P_R04_E02
   * @param filename
   * @return
   */
  public void parseFilename(String filename) {
    Pattern p = Pattern.compile("(\\d\\d\\d\\d)(\\d\\d\\d)(\\d\\d)(\\d\\d)(\\d\\d)_(\\d\\d\\d\\d\\d).+");
    Matcher m = p.matcher(filename);
    if(!m.matches()) {
      throw new RuntimeException("Unexpected filename pattern: " + filename);
    }
    
    time = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    time.set(Calendar.YEAR, Integer.parseInt(m.group(1)));
    time.set(Calendar.DAY_OF_YEAR, Integer.parseInt(m.group(2)));
    time.set(Calendar.HOUR_OF_DAY, 0);
    time.set(Calendar.MINUTE, 0);
    time.set(Calendar.SECOND, 0);
    time.set(Calendar.MILLISECOND, 0);
    
    orbitNo = Integer.parseInt(m.group(6));
  }

  @Override
  public void open(String filename) {
    super.open(filename);
    parseFilename(hdf.getName());
    try {
      time.add(Calendar.MILLISECOND, (int) Math.round(getDouble("UTC_start", 0) * 1000));
    } catch (UnsupportedDataTypeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @Override
  protected void readData(Group root) {
    try {
      readGroupDatasets(getGroup(root, "2B-GEOPROF", "Geolocation Fields"), this.dataFields, 
          null);
      readGroupDatasets(getGroup(root, "2B-GEOPROF", "Data Fields"), this.dataFields, 
          null);
      readGroupDatasets(getGroup(root, "2B-GEOPROF", "Swath Attributes"), this.dataFields, 
          null);
      this.length = ((float[])this.dataFields.get("Latitude").getValues()).length;      
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
      this.currentKey[0] = IntersectUtils.scaleLatLon(getDouble("Latitude", currentIndex));
      this.currentKey[1] = IntersectUtils.scaleLatLon(getDouble("Longitude", currentIndex));
      IntersectUtils.computeActualTime(this.currentKey, this.time, getDouble("Profile_time", currentIndex));
    } catch(Exception ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
    return this.currentKey;
  }

  @Override
  public double[] getAttributes() {
	    try {
        this.currentValue[0] = getAvgDouble("Height", currentIndex);
        this.currentValue[1] = getDouble("Range_to_intercept", currentIndex);
        this.currentValue[2] = getDouble("DEM_elevation", currentIndex);
        this.currentValue[3] = getDouble("Vertical_binsize", currentIndex);
        this.currentValue[4] = getDouble("Pitch_offset", currentIndex);
        this.currentValue[5] = getDouble("Roll_offset", currentIndex);
        this.currentValue[6] = getDouble("Radar_Reflectivity", currentIndex, 1);
      } catch (UnsupportedDataTypeException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

	    return this.currentValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    //Write filename
    WritableUtils.writeCompressedString(out, this.filename);
    
    //Write length
    WritableUtils.writeVInt(out, this.length);
    
    //Write data fields
    writeData(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.filename = WritableUtils.readCompressedString(in);
    parseFilename(this.filename);
    
    this.length = WritableUtils.readVInt(in);
    this.currentIndex = -1;
    
    readData(in);
    
    try {
      time.add(Calendar.MILLISECOND, (int) Math.round(getDouble("UTC_start", 0) * 1000));
    } catch (UnsupportedDataTypeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public double[] getRecord() {
    try {
      
      this.currentRecord[0] = getDouble("Latitude", currentIndex);
      this.currentRecord[1] = getDouble("Longitude", currentIndex);
      IntersectUtils.computeActualTime(this.currentRecord, this.time, getDouble("Profile_time", currentIndex));
      this.currentRecord[5] = orbitNo;
      
      this.currentRecord[6] = getAvgDouble("Height", currentIndex);
      this.currentRecord[7] = getDouble("Range_to_intercept", currentIndex);
      this.currentRecord[8] = getDouble("DEM_elevation", currentIndex);
      this.currentRecord[9] = getDouble("Vertical_binsize", currentIndex);
      this.currentRecord[10] = getDouble("Pitch_offset", currentIndex);
      this.currentRecord[11] = getDouble("Roll_offset", currentIndex);
      this.currentRecord[12] = getDouble("Radar_Reflectivity", currentIndex, 1);
      
      this.currentRecord[13] = (double) getTimestamp((int)this.currentRecord[2], (int)this.currentRecord[3], this.currentRecord[4]);
    } catch (UnsupportedDataTypeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return this.currentRecord;
  }
}
