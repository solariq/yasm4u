package functional;

import com.spbsu.commons.seq.CharSeqTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRReduceMethod;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.Iterator;
import java.util.List;

import static ru.yandex.se.yasm4u.domains.mr.MRTestUtils.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class MapReduceTest extends BaseMRTest {

  private final MRRecord[] RECORDS = createRecords(50); // should be odd

  private static final String IN_TABLE_NAME = TABLE_NAME_PREFIX + "MapReduceTest-1-" + SALT;
  private static final String TEMP_TABLE_NAME = "temp:mr:///MapReduceTest-1-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "MapReduceTest-2-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME, RECORDS);
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class MapReduce {

    private int counter;

    public MapReduce(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME, output = TEMP_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      if (counter % 2 == 0) {
        output.add("odd", "#", "1");
      } else {
        output.add("even", "#", "1");
      }
      counter++;
    }

    @MRReduceMethod(input = TEMP_TABLE_NAME, output = OUT_TABLE_NAME)
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
    List<MRRecord> records = readRecords(env, OUT_TABLE_NAME);
    assertEquals(2, records.size());
    assertEquals(RECORDS.length / 2, CharSeqTools.parseInt(records.get(0).value));
    assertEquals(RECORDS.length / 2, CharSeqTools.parseInt(records.get(1).value));
  }

  @After
  public void dropTable() {
    env.delete(MRPath.createFromURI(IN_TABLE_NAME));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME));
  }

}
