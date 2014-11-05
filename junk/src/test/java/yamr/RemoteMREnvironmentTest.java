package yamr;

import java.util.*;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.*;
import com.spbsu.commons.seq.regexp.SimpleRegExp;
import org.junit.Test;
import solar.mr.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 13:09
 */
public class RemoteMREnvironmentTest {
  final MREnvironment testEnvironment = new RemoteMREnvironment("dodola", "/Berkanavt/mapreduce/bin/mapreduce-dev");

  @Test
  public void testUserSessionsHead() throws Exception {
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("MSK"));
    calendar.set(2014, Calendar.SEPTEMBER, 10);
    final Date start = calendar.getTime();
    final Date end = calendar.getTime();
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.head(new DailyMRTable("user_sessions", start, end), 100, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg);
      }
    });
    final CharSeq result = builder.build();
    assertTrue(result.length() > 100);
    assertEquals(50, SimpleRegExp.create("type=REQUEST").count(result));
    assertEquals(50, SimpleRegExp.create("type=ACCESS").count(result));
  }

  @Test
  public void testSAPPCount() throws Exception {
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("MSK"));
    calendar.set(2014, Calendar.SEPTEMBER, 10);
    final Date start = calendar.getTime();
    final Date end = calendar.getTime();
    final FixedMRTable tempTable1 = new FixedMRTable("tmp/sapp-counter");
    final FixedMRTable tempTable2 = new FixedMRTable("tmp/sapp-counter-result");
//    testEnvironment.setMRServer("kant.yt.yandex.net:80");
    testEnvironment.execute(SAPPCounterMap.class, new DailyMRTable("user_sessions", start, end), tempTable1);
    testEnvironment.sort(tempTable1);
    testEnvironment.execute(SAPPCounterReduce.class, tempTable1, tempTable2);
    testEnvironment.delete(tempTable1);
    final int[] count = new int[]{0};
    testEnvironment.read(tempTable2, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        count[0] = CharSeqTools.parseInt(arg);
      }
    });
    testEnvironment.delete(tempTable2);
    assertEquals(50, count[0]);
  }


}
