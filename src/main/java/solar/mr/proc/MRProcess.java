package solar.mr.proc;

import java.util.List;


import com.spbsu.commons.func.Processor;
import solar.mr.MREnv;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:13
 */
public interface MRProcess {
  String name();

  MRWhiteboard wb();
  MRState execute();

  String[] goals();

  <T> T result();
}
