package solar.mr;

import java.io.Reader;


import com.spbsu.commons.func.Processor;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.MRStateImpl;
import solar.mr.tables.FixedMRTable;
import solar.mr.tables.MRTableShard;

/**
 * User: solar
 * Date: 17.10.14
 * Time: 10:56
 */
public interface MREnv {
  boolean execute(Class<? extends MRRoutine> exec, MRState state, MRTable[] in, MRTable[] out, final MRErrorsHandler errorsHandler);
  boolean execute(Class<? extends MRRoutine> exec, MRState state, MRTable in, MRTable... out);

  MRTableShard[] shards(MRTable table);

  MRTableShard resolve(String path);
  int read(MRTableShard shard, Processor<CharSequence> seq);
  void write(MRTableShard shard, Reader content);
  void append(MRTableShard shard, Reader content);
  void delete(MRTableShard shard);
  void sample(MRTableShard shard, Processor<CharSequence> seq);

  void sort(MRTableShard shard);

  String name();
}
