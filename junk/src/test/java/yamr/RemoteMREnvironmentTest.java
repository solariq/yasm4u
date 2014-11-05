package yamr;

import java.text.MessageFormat;
import java.util.*;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.*;
import com.spbsu.commons.seq.regexp.SimpleRegExp;
import org.junit.Test;
import solar.mr.*;
import solar.mr.env.RemoteYaMREnvironment;
import solar.mr.proc.impl.MRStateImpl;
import solar.mr.tables.DailyMRTable;
import solar.mr.tables.FixedMRTable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 13:09
 */
public abstract class RemoteMREnvironmentTest {
  final MREnv testEnvironment = new RemoteYaMREnvironment("dodola", "/Berkanavt/mapreduce/bin/mapreduce-dev", "cedar:8013", "mobilesearch");

  @Test
  public void testUserSessionsHead() throws Exception {
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("MSK"));
    calendar.set(2014, Calendar.SEPTEMBER, 10);
    final Date start = calendar.getTime();
    final Date end = calendar.getTime();
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.sample(testEnvironment.shards(new DailyMRTable("user_sessions", new MessageFormat("user_sessions/{0,date,yyyyMMdd}"), start, end))[0], new Processor<CharSequence>() {
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
    testEnvironment.execute(
        SAPPCounterMap.class,
        new MRStateImpl(),
        new MRTable[]{new DailyMRTable("user_sessions", new MessageFormat("user_sessions/{0,date,yyyyMMdd}"), start, end)},
        new MRTable[]{tempTable1},
        null);
    tempTable1.sort(testEnvironment);
    tempTable1.sort(testEnvironment);
    testEnvironment.execute(SAPPCounterReduce.class, new MRStateImpl(), tempTable1, tempTable2);
    tempTable1.delete(testEnvironment);
    final int[] count = new int[]{0};
    tempTable2.read(testEnvironment, new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        final CharSequence[] split = CharSeqTools.split(arg, "\t");
        if (split[0].equals("SAPP"))
          count[0] = CharSeqTools.parseInt(split[2]);
      }
    });
    tempTable2.delete(testEnvironment);
    assertEquals(50, count[0]);
  }


}
