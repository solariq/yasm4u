package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MROutput;
import solar.mr.proc.AnnotatedMRProcess;
import solar.mr.proc.State;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRReduceMethod;
import solar.mr.routines.MRRecord;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static solar.mr.MRTestUtils.*;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class SimpleReduceTest extends BaseMRTest {

  private final Record[] RECORDS = createRecordsWithKeys(3, "key1", "key2", "key3");

  private static final String IN_TABLE_NAME = TABLE_NAME_PREFIX + "SimpleReduceTest-1-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "SimpleReduceTest-2-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME, RECORDS);
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class Reduce {

    public Reduce(State state) {
    }

    @MRReduceMethod(input = SCHEMA + IN_TABLE_NAME, output = SCHEMA + OUT_TABLE_NAME)
    public void reduce(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      StringBuilder sks = new StringBuilder();
      StringBuilder vs = new StringBuilder();
      while(reduce.hasNext()) {
        MRRecord record = reduce.next();
        sks.append(record.sub);
        vs.append(record.value);
      }
      output.add(key, sks.toString(), vs.toString());
    }

  }

  @Test
  public void reduceShouldWork() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Reduce.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<Record> records = readRecords(env, OUT_TABLE_NAME);
    assertEquals(3, records.size());
  }

  @After
  public void dropTable() {
    dropMRTable(env, IN_TABLE_NAME);
    dropMRTable(env, OUT_TABLE_NAME);
  }

}
