package solar.mr;

import java.io.Reader;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.WeakListenerHolder;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;


/**
 * User: solar
 * Date: 17.10.14
 * Time: 10:56
 */
public interface MREnv {
  boolean execute(MRRoutineBuilder exec, final MRErrorsHandler errorsHandler);

  MRTableState resolve(MRPath path);
  MRTableState[] resolveAll(MRPath... strings);

  MRPath[] list(MRPath prefix);
  void get(MRPath prefix);

  int read(MRPath shard, Processor<MRRecord> seq);
  void sample(MRPath shard, Processor<MRRecord> seq);
  void write(MRPath shard, Reader content);
  void append(MRPath shard, Reader content);
  void copy(MRPath[] from, MRPath to, boolean append);
  void delete(MRPath shard);
  void sort(MRPath shard);

  String name();
}
