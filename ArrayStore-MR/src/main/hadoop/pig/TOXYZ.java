package hadoop.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class TOXYZ extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(Tuple input) throws IOException {
//    DataBag latlon = (DataBag) input.get(0);
//    double latitude = latlon.;
//    double longitude = latlon.get(1);
    return null;
  }

}
