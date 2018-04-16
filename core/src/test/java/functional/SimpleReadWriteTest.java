package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.expleague.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import com.expleague.yasm4u.domains.mr.routines.ann.tags.MRRead;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.expleague.yasm4u.domains.mr.MRTestUtils.createRecords;
import static com.expleague.yasm4u.domains.mr.MRTestUtils.readRecords;
import static com.expleague.yasm4u.domains.mr.MRTestUtils.writeRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by inikifor on 02.12.14.
 */
@RunWith(Parameterized.class)
public final class SimpleReadWriteTest extends BaseMRTest {

  private final MRRecord[] RECORDS = createRecords(3);

  private static final String TABLE_NAME = TABLE_NAME_PREFIX + "SimpleReadWriteTest-" + SALT;
  private static final String SEPARATOR = "#";
  private static final String RESULT = "var:result";

  @Before
  public void createTable() {
    writeRecords(env, TABLE_NAME, RECORDS);
  }

  private void checkRecords(List<MRRecord> records) {
    assertEquals(3, records.size());
    for(MRRecord record: RECORDS) {
      assertTrue(records.contains(record));
    }
  }

  @Test
  public void readingFromEnvShouldWork() {
    checkRecords(readRecords(env, TABLE_NAME));
  }

  @MRProcessClass(goal = RESULT)
  public static final class Reader {

    public Reader(State state) {
    }

    @MRRead(input = TABLE_NAME, output = RESULT)
    public List<MRRecord> read(Iterator<MRRecord> lines) {
      final List<MRRecord> result = new ArrayList<>();
      while(lines.hasNext()) {
        result.add(lines.next());
      }
      return result;
    }

  }

  @Test
  public void readingFromProcessShouldWork() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Reader.class, env);
    checkRecords(mrProcess.<List<MRRecord>>result());
  }

  @After
  public void dropTable() {
    env.delete(MRPath.createFromURI(TABLE_NAME));
  }

}
