package ru.yandex.se.yasm4u.domains.mr.routines;

import com.spbsu.commons.util.MultiMap;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MRPath;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: solar
 * Date: 07.11.14
 * Time: 16:39
 */
public class SortRoutine implements Routine {
  public static final String MERGE_DELIM = "+";

  private static final Pattern MERGE_PATTERN = Pattern.compile("^(.*)\\" + MERGE_DELIM + "\\d+$");

  @Override
  public Joba[] buildVariants(Ref[] state, final JobExecutorService executor) {
    final List<Joba> variants = new ArrayList<>();
    for(int i = 0; i < state.length; i++) {
      if (MRPath.class.isAssignableFrom(state[i].type())) {
        final MRPath ref = (MRPath)state[i].resolve(executor);
        if (!ref.sorted)
          variants.add(new Joba.Stub(new Ref[]{ref}, new Ref[]{ref.mksorted()}) {
            @Override
            public void run() {
              executor.domain(MREnv.class).sort((MRPath)consumes()[0].resolve(executor));
            }
          });
      }
    }
    return variants.toArray(new Joba[variants.size()]);
  }
}
