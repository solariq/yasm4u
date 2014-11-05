package solar.mr;

import com.spbsu.commons.seq.Seq;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 13:36
 */
public interface MRTable extends Seq<String> {
  String name();
}
