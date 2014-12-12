package solar.mr.proc.impl;

import com.spbsu.commons.func.Processor;
import solar.mr.MRTableShard;
import solar.mr.proc.Joba;
import solar.mr.proc.Whiteboard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * User: solar
 * Date: 07.11.14
 * Time: 16:39
 */
public class MergeJoba implements Joba {
  private final String[] shards;
  private final String result;

  public MergeJoba(final String[] shards, final String result) {
    this.shards = shards;
    this.result = result;
  }

  @Override
  public String name() {
    return toString();
  }

  @Override
  public boolean run(final Whiteboard wb) {
    final List<MRTableShard> shards = new ArrayList<>();

    for(int i = 0; i < this.shards.length; i++) {
      wb.processAs(this.shards[i], new Processor<MRTableShard>() {
        @Override
        public void process(MRTableShard arg) {
          shards.add(arg);
        }
      });
    }
    wb.env().copy(shards.toArray(new MRTableShard[shards.size()]), wb.<MRTableShard>get(result), false);
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

  @Override
  public String toString() {
    return "Merge " + Arrays.toString(consumes()) + " -> " + result;
  }
}
