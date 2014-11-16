package solar.mr.proc.impl;

import com.spbsu.commons.func.Processor;
import solar.mr.MRTableShard;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRWhiteboard;

/**
 * User: solar
 * Date: 07.11.14
 * Time: 16:39
 */
public class MergeJoba implements MRJoba {
  private final String[] shards;
  private final String result;

  public MergeJoba(final String[] shards, final String result) {
    this.shards = shards;
    this.result = result;
  }

  @Override
  public boolean run(final MRWhiteboard wb) {
    for (String shard : shards) {
      final MRTableShard result = wb.resolve(this.result);
      if (!wb.processAs(shard, new Processor<MRTableShard>() {
        @Override
        public void process(MRTableShard arg) {
          wb.env().copy(arg, result, true);
        }
      })) {
        throw new IllegalArgumentException("Unsupported type for merge");
      }
    }
    return true;
  }

  @Override
  public String[] consumes() {
    return shards;
  }

  @Override
  public String[] produces() {
    return new String[]{result};
  }
}
