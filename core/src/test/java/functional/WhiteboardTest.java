package functional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.expleague.yasm4u.domains.wb.Whiteboard;
import com.expleague.yasm4u.domains.wb.impl.WhiteboardImpl;

import java.util.*;

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
  
  @Test
  public void list() {
    ArrayList<String> l1 = new ArrayList<>();
    l1.add("one");
    l1.add("two");
    
    Whiteboard wb = new WhiteboardImpl(env, "TEST");
    wb.set("var:list", l1);
    wb.snapshot();
    List<?> l2 = wb.get("var:list");
    assertEquals(l1.size(), l2.size());
    for (Iterator<?> i1 = l1.iterator(),i2 = l2.iterator(); i1.hasNext() && i2.hasNext(); ) {
      assertEquals(i1.next(), i2.next());
    }
    wb.wipe();
  }
}
