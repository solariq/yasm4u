package solar.mr.proc.impl;

import solar.mr.MRTableShard;
import solar.mr.proc.MRJoba;
import solar.mr.proc.MRWhiteboard;

import java.util.List;

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
      final Object shardContent = wb.refresh(shard);
      if (shardContent instanceof MRTableShard) {
        final MRTableShard result = wb.refresh(this.result);
        wb.env().copy(((MRTableShard) shardContent), result, true);
      }
      else throw new IllegalArgumentException("Unsupported type for merge");
    }
    return true;
  }

  @Override
  public String[] consumes(MRWhiteboard wb) {
    final String[] result = new String[shards.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = wb.resolveName(shards[i]);
    }
    return result;
  }

  @Override
  public String[] produces(MRWhiteboard wb) {
    return new String[]{wb.resolveName(result)};
  }
}