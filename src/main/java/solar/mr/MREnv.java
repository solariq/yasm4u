package solar.mr;

import java.io.Reader;
import java.net.URI;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.WeakListenerHolder;
import solar.mr.proc.MRState;

/**
 * User: solar
 * Date: 17.10.14
 * Time: 10:56
 */
public interface MREnv extends WeakListenerHolder<MREnv.ShardAlter> {
  boolean execute(Class<? extends MRRoutine> exec, MRState state, MRTableShard[] in, MRTableShard[] out, final MRErrorsHandler errorsHandler);

  MRTableShard resolve(String path);
  MRTableShard[] resolveAll(String[] strings);

  int read(MRTableShard shard, Processor<CharSequence> seq);
  void write(MRTableShard shard, Reader content);
  void append(MRTableShard shard, Reader content);
  void delete(MRTableShard shard);
  void sample(MRTableShard shard, Processor<CharSequence> seq);

  void copy(MRTableShard from, MRTableShard to, boolean append);

  MRTableShard sort(MRTableShard shard);

  String name();

  class ShardAlter {
    public final MRTableShard shard;
    public final AlterType type;

    public ShardAlter(MRTableShard shard) {
      this(shard, AlterType.CHANGED);
    }
    public ShardAlter(MRTableShard shard, AlterType type) {
      this.shard = shard;
      this.type = type;
    }

    public enum AlterType {
      CHANGED, UPDATED
    }
  }
}
