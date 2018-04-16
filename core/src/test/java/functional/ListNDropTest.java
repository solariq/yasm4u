package functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.expleague.yasm4u.domains.mr.env.LocalMREnv;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import static com.expleague.yasm4u.domains.mr.MRTestUtils.createRecords;
import static com.expleague.yasm4u.domains.mr.MRTestUtils.writeRecords;
import static org.junit.Assert.assertEquals;

/**
 * Created by inikifor on 04.12.14.
 */
@RunWith(Parameterized.class)
public final class ListNDropTest extends BaseMRTest {

  private final MRRecord[] RECORDS_1 = createRecords(1);
  private final MRRecord[] RECORDS_2 = createRecords(1);
  private final MRRecord[] RECORDS_3 = createRecords(1);

  private static final String IN_TABLE_NAME_PREFIX = TABLE_NAME_PREFIX + "list/";
  private static final String IN_TABLE_NAME_1 = IN_TABLE_NAME_PREFIX + "ListTest-1-" + SALT;
  private static final String IN_TABLE_NAME_2 = IN_TABLE_NAME_PREFIX + "ListTest-2-" + SALT;
  private static final String IN_TABLE_NAME_3 = IN_TABLE_NAME_PREFIX + "ListTest-3-" + SALT;

  @Before
  public void createTable() {
    writeRecords(env, IN_TABLE_NAME_1, RECORDS_1);
    writeRecords(env, IN_TABLE_NAME_2, RECORDS_2);
    writeRecords(env, IN_TABLE_NAME_3, RECORDS_3);
  }

  @Test
  public void listShouldWork() {
    MRPath[] result = env.list(MRPath.createFromURI(IN_TABLE_NAME_PREFIX));
    assertEquals(3, result.length);
  }

  @Test
  public void dropShouldWork() {
    MRPath[] result = env.list(MRPath.createFromURI(IN_TABLE_NAME_PREFIX));
    assertEquals(3, result.length);
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_1));
    result = env.list(MRPath.createFromURI(IN_TABLE_NAME_PREFIX));
    assertEquals(2, result.length);
  }

  @Test
  public void notExistance() {
    MRPath[] result = env.list(MRPath.create("/home/__no_existant/"));
    assertEquals(0, result.length);
    env.sample(MRPath.create("/home/__no_existant"), arg -> {});
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void exceptionNotExistance() {
    if (env instanceof LocalMREnv) /* there're no resolve in the middle */
      throw new IllegalArgumentException("It's expected!!!");
    env.sample(MRPath.create("/home/__no_existant/"), arg -> {});
  }

  @Test
  public void testNestedNotExistance() {
    env.sample(MRPath.create("/home/__no_existant/__no_existant"), arg -> {

    });

    env.sample(MRPath.create("/log/__no_existant/__no_existant"), arg -> {});
  }

  @After
  public void dropTable() {
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_1));
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_2));
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_3));
    env.delete(MRPath.createFromURI(IN_TABLE_NAME_PREFIX));
  }

}
