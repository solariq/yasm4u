package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.MRTableShard;

import static functional.MRTestUtils.createRecords;
import static functional.MRTestUtils.dropMRTable;
import static functional.MRTestUtils.writeRecords;
import static org.junit.Assert.assertEquals;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class ListNDropTest extends BaseMRTest {

  private final MRTestUtils.Record[] RECORDS_1 = createRecords(1);
  private final MRTestUtils.Record[] RECORDS_2 = createRecords(1);
  private final MRTestUtils.Record[] RECORDS_3 = createRecords(1);

  private static final String IN_TABLE_NAME_1 = TABLE_NAME_PREFIX + "ListTest-1-" + SALT;
  private static final String IN_TABLE_NAME_2 = TABLE_NAME_PREFIX + "ListTest-2-" + SALT;
  private static final String IN_TABLE_NAME_3 = TABLE_NAME_PREFIX + "ListTest-3-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME_1, RECORDS_1);
    writeRecords(env, IN_TABLE_NAME_2, RECORDS_2);
    writeRecords(env, IN_TABLE_NAME_3, RECORDS_3);
  }

  @Test
  public void listShouldWork() {
    MRTableShard[] result = env.list(TABLE_NAME_PREFIX);
    assertEquals(3, result.length);
  }

  @Test
  public void dropShouldWork() {
    MRTableShard[] result = env.list(TABLE_NAME_PREFIX);
    assertEquals(3, result.length);
    dropMRTable(env, IN_TABLE_NAME_1);
    result = env.list(TABLE_NAME_PREFIX);
    assertEquals(2, result.length);
  }

  @After
  public void dropTable() {
    dropMRTable(env, IN_TABLE_NAME_1);
    dropMRTable(env, IN_TABLE_NAME_2);
    dropMRTable(env, IN_TABLE_NAME_3);
  }

}
