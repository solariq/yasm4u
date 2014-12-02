package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static functional.MRTestUtils.*;

/**
 * Created by inikifor on 02.12.14.
 */
@RunWith(Parameterized.class)
public final class SimpleReadWriteTest extends BaseMRTest {

  private final Record[] RECORDS = {
      new Record("key1", "sub1", "value1"),
      new Record("key2", "sub2", "value2"),
      new Record("key3", "sub3", "value3")
  };

  @Before
  public void createTable() {
    writeRecords(env, tableName(), RECORDS);
  }

  @Test
  public void readingFromEnvShouldWork() {
    List<Record> records = readRecords(env, tableName());
    assertEquals(3, records.size());
    for(Record record: RECORDS) {
      assertTrue(records.contains(record));
    }
  }

  @After
  public void dropTable() {
    dropMRTable(env, tableName());
  }

}
