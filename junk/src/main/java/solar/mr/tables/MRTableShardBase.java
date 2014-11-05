package solar.mr.tables;

import java.util.Date;


import solar.mr.MREnv;
import solar.mr.MRTable;

/**
* User: solar
* Date: 15.10.14
* Time: 11:08
*/
public abstract class MRTableShardBase implements MRTableShard {
  private final String path;
  private final MREnv container;
  private final MRTable owner;
  private final long metaTS;

  public MRTableShardBase(final String path, final MREnv container, final MRTable owner) {
    this.path = path;
    this.container = container;
    this.owner = owner;
    metaTS = System.currentTimeMillis();
  }

  @Override
  public MRTable owner() {
    return owner;
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public MREnv container() {
    return container;
  }

  @Override
  public long metaTS() {
    return metaTS;
  }
}
