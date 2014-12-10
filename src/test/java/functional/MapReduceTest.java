package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MROutput;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.AnnotatedMRProcess;
import solar.mr.proc.tags.MRMapMethod;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRReduceMethod;
import solar.mr.routines.MRRecord;

import java.util.Iterator;
import java.util.List;

import static solar.mr.MRTestUtils.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class MapReduceTest extends BaseMRTest {

  private final Record[] RECORDS = createRecords(50); // should be odd

  private static final String IN_TABLE_NAME = TABLE_NAME_PREFIX + "MapReduceTest-1-" + SALT;
  private static final String TEMP_TABLE_NAME = "MapReduceTest-1-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "MapReduceTest-2-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME, RECORDS);
  }

  @MRProcessClass(goal = SCHEMA + OUT_TABLE_NAME)
  public static final class MapReduce {

    private int counter;

    public MapReduce(MRState state) {
    }

    @MRMapMethod(input = SCHEMA + IN_TABLE_NAME, output = TMP_SCHEMA + TEMP_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      if (counter % 2 == 0) {
        output.add("odd", "#", "1");
      } else {
        output.add("even", "#", "1");
      }
      counter++;
    }

    @MRReduceMethod(input = TMP_SCHEMA + TEMP_TABLE_NAME, output = SCHEMA + OUT_TABLE_NAME)
    public void reduce(final String key, final Iterator<MRRecord> reduce, final MROutput output) {
      int count = 0;
      while(reduce.hasNext()) {
        MRRecord record = reduce.next();
        count++;
      }
      output.add(key, "#", "" + count);
    }

  }

  @Test
  public void mapReduceScriptShouldWork() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(MapReduce.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<Record> records = readRecords(env, OUT_TABLE_NAME);
    assertEquals(2, records.size());
    assertEquals("" + RECORDS.length / 2, records.get(0).value);
    assertEquals("" + RECORDS.length / 2, records.get(1).value);
  }

  @After
  public void dropTable() {
    dropMRTable(env, IN_TABLE_NAME);
    dropMRTable(env, OUT_TABLE_NAME);
  }

}
