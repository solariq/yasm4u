package solar.mr;

import com.spbsu.commons.func.Processor;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 10:42
 */
public class FixedMRTable implements MRTable {
  private final String name;

  public FixedMRTable(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void visitShards(final Processor<String> shardNameProcessor) {
    shardNameProcessor.process(name);
  }
}
