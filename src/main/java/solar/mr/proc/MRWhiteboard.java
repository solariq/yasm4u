package solar.mr.proc;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.types.SerializationRepository;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;
import solar.mr.MRTableShard;

import java.util.Set;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public interface MRWhiteboard extends MRState {
  MRState snapshot();

  <T> void set(String var, T data);
  void remove(String var);

  void wipe();

  // to ext
  MRErrorsHandler errorsHandler();
  MREnv env();
}
