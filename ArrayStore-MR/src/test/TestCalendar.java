import intersect.hdf.IntersectUtils;

import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Test;


public class TestCalendar {
  
  @Test
  public void testAddMili() {
    Calendar cal = Calendar.getInstance();
    cal.set(2007, 0, 1, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    
    System.out.println("Before:");
    System.out.println(cal.getTime());
    System.out.println(cal.get(Calendar.MILLISECOND));
    
    cal.add(Calendar.MILLISECOND, IntersectUtils.MSECONDS_IN_DAY - 1);
    
    System.out.println("After - 1:");
    System.out.println(cal.getTime());
    System.out.println(cal.get(Calendar.MILLISECOND));
    
    cal.add(Calendar.MILLISECOND, 2);
    
    System.out.println("After + 2:");
    System.out.println(cal.getTime());
    System.out.println(cal.get(Calendar.MILLISECOND));
  }
  
  @Test
  public void testGMTTimeZone() {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    cal.set(2007, 0, 1, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    
    System.out.println(cal.getTimeZone());
    
    System.out.println("Before:");
    System.out.printf("%d-%d-%d %d:%d:%d %d\n", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DATE),
        cal.get(Calendar.HOUR), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND));
    System.out.println("Local Time: " + cal.getTime());
    System.out.println(cal.get(Calendar.MILLISECOND));
    
    cal.add(Calendar.MILLISECOND, -1);
    
    System.out.println("After - 1:");
    System.out.printf("%d-%d-%d %d:%d:%d %d\n", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DATE),
        cal.get(Calendar.HOUR), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND));
    System.out.println("Local Time: " + cal.getTime());
    System.out.println(cal.get(Calendar.MILLISECOND));
  }
  
  @Test
  public void testPrintFormat() {
    System.out.println(String.format("%.4f", 5.2323223).replaceAll("\\.?0+$", ""));
    System.out.println(String.format("%.4f", 5.23).replaceAll("\\.?0+$", ""));
    System.out.println(String.format("%.4f", 5.2).replaceAll("\\.?0+$", ""));
    System.out.println(String.format("%.4f", 5f).replaceAll("\\.?0+$", ""));
  }
}
