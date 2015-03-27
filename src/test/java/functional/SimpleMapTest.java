package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ru.yandex.se.yasm4u.domains.mr.MRTestUtils.*;

/**
 * Created by inikifor on 03.12.14.
 */
@RunWith(Parameterized.class)
public final class SimpleMapTest extends BaseMRTest {

  private final MRRecord[] RECORDS = createRecords(10); // should be odd

  private static final String IN_TABLE_NAME = TABLE_NAME_PREFIX + "SimpleMapTest-1-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "SimpleMapTest-2-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME, RECORDS);
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class Map {

    private int counter;

    public Map(State state) {
    }

    @MRMapMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      if (counter % 2 == 0) {
        output.add(key, sub, value);
      }
      counter++;
    }

  }

  @Test
  public void mapWithStateShouldWork() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Map.class, env);
    mrProcess.wb().wipe();
    mrProcess.execute();
    mrProcess.wb().wipe();
    List<MRRecord> records = readRecords(env, OUT_TABLE_NAME);
    assertEquals(RECORDS.length / 2, records.size());
    Set<MRRecord> recordsSet = new HashSet<>(Arrays.asList(RECORDS));
    assertTrue(recordsSet.containsAll(records));
  }

  @After
  public void dropTable() {
    env.delete(MRPath.createFromURI(IN_TABLE_NAME));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME));
  }

}
