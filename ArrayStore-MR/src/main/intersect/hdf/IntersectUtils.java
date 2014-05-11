package intersect.hdf;

import java.util.Calendar;

public class IntersectUtils {
  public static int SECONDS_IN_DAY = 60*60*24;
  public static int MSECONDS_IN_DAY = 60*60*24*1000;
  
  public static int scaleLatLon(double latOrLon) {
    return (int) Math.round(latOrLon * 10000);
  }
  
  public static int scaleTime(double time) {
    return (int) Math.round(time * 1000);
  }
  
  /**
   * @param year
   * @param doy
   * @param time
   * @return the actual year, day and seconds offset from the start of the dya
   */
  public static int[] getActualTime(int year, int doy, double time) {
    int[] t = new int[3];
    t[1] = doy + (time >= SECONDS_IN_DAY ? 1 : 0);
    if(year % 4 == 0) {
      t[0] = year + (t[1] > 366 ? 1 : 0);
      t[1] = ((t[1] - 1) % 366) + 1;
    } else {
      t[0] = year + (t[1] > 365 ? 1 : 0);
      t[1] = ((t[1] - 1) % 365) + 1;
    }
    t[2] = (int)Math.round(time*1000) % MSECONDS_IN_DAY;
    return t;
  }
  
  public static void computeActualTime(int[] coords, int year, int doy, double time) {
    coords[3] = doy + (time >= SECONDS_IN_DAY ? 1 : 0);
    if(year % 4 == 0) {
      coords[2] = year + (coords[3] > 366 ? 1 : 0);
      coords[3] = ((coords[3] - 1) % 366) + 1;
    } else {
      coords[2] = year + (coords[3] > 365 ? 1 : 0);
      coords[3] = ((coords[3] - 1) % 365) + 1;
    }
    coords[4] = (int)Math.round(time*1000) % MSECONDS_IN_DAY;
    //coords[4] = (int)Math.round(time*100) % (SECONDS_IN_DAY*100);
  }
  
  /**
   * @param coords the coordinates array, where the 3-5 entries are year, day of year, and milliseconds from start of day
   * @param d the calendar object for the start of the day
   * @param time the seconds offset from the start of the day
   */
  public static void computeActualTime(long[] coords, Calendar d, double time) {
    Calendar cd = (Calendar) d.clone();
    int s = (int)Math.round(time*1000);
    cd.add(Calendar.MILLISECOND, s);
    coords[2] = cd.get(Calendar.YEAR);
    coords[3] = cd.get(Calendar.DAY_OF_YEAR);
    coords[4] = (int)(cd.getTimeInMillis() % MSECONDS_IN_DAY);
  }
  
  public static void computeActualTime(double[] record, Calendar d, double time) {
    Calendar cd = (Calendar) d.clone();
    int s = (int)Math.round(time*1000);
    cd.add(Calendar.MILLISECOND, s);
    record[2] = cd.get(Calendar.YEAR);
    record[3] = cd.get(Calendar.DAY_OF_YEAR);
    //record[4] = cd.get(Calendar.HOUR_OF_DAY)*3600 + cd.get(Calendar.MINUTE) * 60 + cd.get(Calendar.SECOND) + cd.get(Calendar.MILLISECOND)/1000; 
    record[4] = cd.getTimeInMillis() % MSECONDS_IN_DAY;
  }
}
