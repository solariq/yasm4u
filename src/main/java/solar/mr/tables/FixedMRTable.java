package solar.mr.tables;

import solar.mr.MREnv;
import solar.mr.MRTable;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 10:42
 */
public class FixedMRTable extends MRTable.Stub {
  private final String name;

  public FixedMRTable(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean available(final MREnv env) {
    for (MRTableShard shard : env.shards(this)) {
      if (!shard.isAvailable())
        return false;
    }
    return true;
  }

  @Override
  public String crc(final MREnv env) {
    final StringBuilder crc = new StringBuilder();
    for (MRTableShard shard : env.shards(this)) {
      crc.append(shard.crc());
    }
    return crc.toString();
  }
}
