package functional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import solar.mr.env.LocalMREnv;
import solar.mr.proc.Whiteboard;
import solar.mr.proc.impl.WhiteboardImpl;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * Created by minamoto on 25/02/15.
 */
@RunWith(Parameterized.class)
public class WhiteboardTest extends BaseMRTest {
  @Test
  public void hashMap() {
    HashMap<String, Long> map = new HashMap<>();
    map.put("one", 1l);
    map.put("two", 2l);

    Whiteboard wb = new WhiteboardImpl(env, "TEST");
    wb.set("var:test", map);
    wb.snapshot();
    Map<?,?> map1 = wb.get("var:test");
    assertEquals(map.size(), map1.size());
    for (String key:map.keySet()) {
      assertEquals(map.get(key), map1.get(key));
    }
    wb.wipe();
  }
}
