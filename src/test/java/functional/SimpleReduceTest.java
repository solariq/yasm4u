package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import ru.yandex.se.yasm4u.domains.mr.MROutput;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.tags.MRReduceMethod;
import ru.yandex.se.yasm4u.domains.mr.ops.MRRecord;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static ru.yandex.se.yasm4u.domains.mr.MRTestUtils.*;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class SimpleReduceTest extends BaseMRTest {

  private final MRRecord[] RECORDS = createRecordsWithKeys(3, "key1", "key2", "key3");

  private static final String IN_TABLE_NAME = TABLE_NAME_PREFIX + "SimpleReduceTest-1-" + SALT;
  private static final String OUT_TABLE_NAME = TABLE_NAME_PREFIX + "SimpleReduceTest-2-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME, RECORDS);
  }

  @MRProcessClass(goal = OUT_TABLE_NAME)
  public static final class Reduce {

    public Reduce(State state) {
    }

    @MRReduceMethod(input = IN_TABLE_NAME, output = OUT_TABLE_NAME)
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
    List<MRRecord> records = readRecords(env, OUT_TABLE_NAME);
    assertEquals(3, records.size());
  }

  @After
  public void dropTable() {
    env.delete(MRPath.createFromURI(IN_TABLE_NAME));
    env.delete(MRPath.createFromURI(OUT_TABLE_NAME));
  }

}
