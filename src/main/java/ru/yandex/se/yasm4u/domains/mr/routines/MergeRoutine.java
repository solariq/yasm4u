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
public class MergeRoutine implements Routine {
  public static final String MERGE_DELIM = "+";

  private static final Pattern MERGE_PATTERN = Pattern.compile("^(.*)\\" + MERGE_DELIM + "\\d+$");

  @Override
  public Joba[] buildVariants(Ref[] state, final JobExecutorService executor) {
    final MultiMap<String, MRPath> map = new MultiMap<>();
    for(int i = 0; i < state.length; i++) {
      if (MRPath.class.isAssignableFrom(state[i].type())) {
        final MRPath ref = (MRPath)state[i].resolve(executor);
        final Matcher matcher = MERGE_PATTERN.matcher(ref.toString());
        if (matcher.find()) {
          map.put(matcher.group(1), ref);
        }
      }
    }
    final Set<String> keys = map.getKeys();
    final Joba[] variants = new Joba[keys.size()];
    final Iterator<String> it = keys.iterator();
    for(int i = 0; i < variants.length; i++) {
      final String result = it.next();
      final Collection<MRPath> paths = map.get(result);
      final MRPath resultPath = MRPath.createFromURI(result);
      variants[i] = new Joba.Stub(paths.toArray(new MRPath[paths.size()]), new MRPath[]{resultPath}) {
        @Override
        public void run() {
          executor.domain(MREnv.class).copy((MRPath[])consumes(), resultPath, false);
        }
        public String toString() {
          return "Merge " + Arrays.toString(consumes()) + " -> " + result;
        }
      };
    }
    return variants;
  }
}
