package solar.mr;

import java.util.Calendar;
import java.util.Date;


import com.spbsu.commons.func.Computable;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.Seq;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 13:36
 */
public interface MRTable  {
  String name();

  void visitShards(Processor<String> shardNameProcessor);

  Computable<String, MRTable> RESOLVER = new Computable<String, MRTable>() {
    @Override
    public MRTable compute(final String argument) {
      if (argument.startsWith("user_sessions/")) {
        final Date date = parseDate(argument.subSequence(argument.indexOf("/") + 1, argument.length()));
        return new DailyMRTable("user_sessions", date, date);
      }
      return new FixedMRTable(argument);
    }

    private Date parseDate(final CharSequence sequence) {
      final int year = Integer.parseInt(sequence.subSequence(0, 4).toString());
      final int month = Integer.parseInt(sequence.subSequence(4, 6).toString());
      final int day = Integer.parseInt(sequence.subSequence(6, 8).toString());
      final Calendar calendar = Calendar.getInstance();
      calendar.set(year, month - 1, day - 1);
      return calendar.getTime();
    }
  };
}
