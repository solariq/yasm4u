package solar.mr;

import java.io.Reader;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.func.WeakListenerHolder;
import solar.mr.proc.State;

/**
 * User: solar
 * Date: 17.10.14
 * Time: 10:56
 */
public interface MREnv extends WeakListenerHolder<MREnv.ShardAlter> {
  boolean execute(Class<? extends MRRoutine> exec, State state, MRTableShard[] in, MRTableShard[] out, final MRErrorsHandler errorsHandler);

  MRTableShard[] list(String prefix);
  MRTableShard resolve(String path);
  MRTableShard[] resolveAll(String... strings);

  int read(MRTableShard shard, Processor<CharSequence> seq);
  void sample(MRTableShard shard, Processor<CharSequence> seq);

  MRTableShard write(MRTableShard shard, Reader content);
  MRTableShard append(MRTableShard shard, Reader content);
  MRTableShard copy(MRTableShard[] from, MRTableShard to, boolean append);
  MRTableShard delete(MRTableShard shard);
  MRTableShard sort(MRTableShard shard);

  /* loacation of temporal resources */
  String tempPrefix();

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
