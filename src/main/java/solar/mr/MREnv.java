package solar.mr;

import java.io.Reader;
import java.net.URI;


import com.spbsu.commons.func.Processor;
import solar.mr.proc.MRState;

/**
 * User: solar
 * Date: 17.10.14
 * Time: 10:56
 */
public interface MREnv {
  boolean execute(Class<? extends MRRoutine> exec, MRState state, MRTableShard[] in, MRTableShard[] out, final MRErrorsHandler errorsHandler);
  MRTableShard resolve(String path);

  int read(MRTableShard shard, Processor<CharSequence> seq);
  void write(MRTableShard shard, Reader content);
  void append(MRTableShard shard, Reader content);
  void delete(MRTableShard shard);
  void sample(MRTableShard shard, Processor<CharSequence> seq);
  void copy(MRTableShard from, MRTableShard to, boolean append);

  MRTableShard sort(MRTableShard shard);

  String name();
}
