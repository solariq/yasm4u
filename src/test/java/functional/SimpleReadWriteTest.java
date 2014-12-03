package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.proc.MRState;
import solar.mr.proc.impl.AnnotatedMRProcess;
import solar.mr.proc.tags.MRProcessClass;
import solar.mr.proc.tags.MRRead;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static functional.MRTestUtils.*;

/**
 * Created by inikifor on 02.12.14.
 */
@RunWith(Parameterized.class)
public final class SimpleReadWriteTest extends BaseMRTest {

  private final Record[] RECORDS = createRecords(3);

  private static final String TABLE_NAME = TABLE_NAME_PREFIX + "SimpleReadWriteTest-" + SALT;
  private static final String SEPARATOR = "#";
  private static final String RESULT = "var:result";

  @Before
  public void createTable() {
    writeRecords(env, TABLE_NAME, RECORDS);
  }

  private void checkRecords(List<Record> records) {
    assertEquals(3, records.size());
    for(Record record: RECORDS) {
      assertTrue(records.contains(record));
    }
  }

  @Test
  public void readingFromEnvShouldWork() {
    checkRecords(readRecords(env, TABLE_NAME));
  }

  @MRProcessClass(goal = RESULT)
  public static final class Reader {

    public Reader(MRState state) {
    }

    @MRRead(input = SCHEMA + TABLE_NAME, output = RESULT)
    public String count(Iterator<CharSequence> line) {
      StringBuilder sb = new StringBuilder();
      while(line.hasNext()) {
        sb.append(new Record(line.next()));
        sb.append(SEPARATOR);
      }
      return sb.toString();
    }

  }

  @Test
  public void readingFromProcessShouldWork() {
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(Reader.class, env);
    String recordsData = mrProcess.result();
    List<Record> records = new ArrayList<>();
    for(String record: recordsData.split(SEPARATOR)) {
      records.add(new Record(record));
    }
    checkRecords(records);
  }

  @After
  public void dropTable() {
    dropMRTable(env, TABLE_NAME);
  }

}
