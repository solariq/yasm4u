package solar.mr;

import java.util.Calendar;
import java.util.Date;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.Seq;

/**
* User: solar
* Date: 25.09.14
* Time: 20:59
*/
public class DailyMRTable implements MRTable {
  private final Date start;
  private final Date end;
  private final int length;
  private String path;

  public DailyMRTable(final String path, final Date start, final Date end) {
    this.path = path;
    this.start = start;
    this.end = end;
    final Calendar instance = Calendar.getInstance();
    instance.setTime(start);
    int count = 0;
    while (instance.before(end) || instance.getTime().equals(end)) {
      instance.add(Calendar.DAY_OF_YEAR, 1);
      count++;
    }
    length = count;
  }

  @Override
  public String name() {
    return path;
  }

  @Override
  public void visitShards(final Processor<String> shardNameProcessor) {
    for (int i = 0; i < length; i++) {
      final Calendar instance = dateAt(i);
      shardNameProcessor.process(name() + "/" + instance.get(Calendar.YEAR) + "" + String.format("%02d", instance.get(Calendar.MONTH) + 1) + "" + (instance.get(Calendar.DAY_OF_MONTH) + 1));
    }
  }

  private Calendar dateAt(final int i) {
    final Calendar instance = Calendar.getInstance();
    instance.setTime(start);
    instance.add(Calendar.DAY_OF_YEAR, i);
    if (end.after(instance.getTime()))
      throw new ArrayIndexOutOfBoundsException(i);
    return instance;
  }
}
