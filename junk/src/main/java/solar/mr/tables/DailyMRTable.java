package solar.mr.tables;

import java.text.MessageFormat;
import java.util.Calendar;
import java.util.Date;


import solar.mr.MRTable;

/**
* User: solar
* Date: 25.09.14
* Time: 20:59
*/
public class DailyMRTable extends MRTable.Stub {
  private final String name;
  private final MessageFormat shardNameFormat;
  private final Date start;
  private final Date end;
  private final int length;

  public DailyMRTable(final String name, final MessageFormat shardNameFormat, final Date start, final Date end) {
    this.name = name;
    this.shardNameFormat = shardNameFormat;
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
    return name;
  }

  public String shardName(final int i) {
    final Calendar instance = Calendar.getInstance();
    instance.setTime(start);
    instance.add(Calendar.DAY_OF_YEAR, i);
    if (end.after(instance.getTime()))
      throw new ArrayIndexOutOfBoundsException(i);

    return shardNameFormat.format(new Object[]{instance.getTime()});
  }

  public int length() {
    return length;
  }
}
