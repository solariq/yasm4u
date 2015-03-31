package ru.yandex.se.yasm4u.domains.mr.routines;

import com.spbsu.commons.util.MultiMap;
import gnu.trove.map.TMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TCustomHashMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import ru.yandex.se.yasm4u.JobExecutorService;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.mr.MRPath;
import ru.yandex.se.yasm4u.domains.mr.routines.ann.impl.RoutineJoba;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: solar
 * Date: 07.11.14
 * Time: 16:39
 */
public class MergeRoutine implements Routine {
  private static final Pattern MERGE_PATTERN = Pattern.compile("^(.*)/merge/([^\\/]+)/(\\d+)-(\\d+)$");

  public static List<RoutineJoba> unmergeJobs(final List<RoutineJoba> jobs) {
    final TObjectIntMap<Ref> sharded = new TObjectIntHashMap<>();
    for (final Joba joba : jobs) {
      for (final Ref resource : joba.produces()) {
        sharded.adjustOrPutValue(resource, 1, 1);
      }
    }

    final List<RoutineJoba> result = new ArrayList<>();

    final TObjectIntMap<MRPath> shardsCount = new TObjectIntHashMap<>();
    for (final RoutineJoba joba : jobs) {
      final MRPath[] outputs = new MRPath[joba.produces().length];
      for(int i = 0; i < outputs.length; i++) {
        final MRPath resourceName = joba.produces()[i];
        if (sharded.get(resourceName) > 1) {
          final MRPath shard = new MRPath(MRPath.Mount.TEMP, resourceName.path + "/merge/" + resourceName.mount + "/" + sharded.get(resourceName) + "-" + shardsCount.get(resourceName), false);
          outputs[i] = shard;
          shardsCount.adjustOrPutValue(resourceName, 1, 1);
        }
        else outputs[i] = resourceName;
      }
      if (!Arrays.equals(outputs, joba.produces())) {
        //noinspection unchecked
        result.add(new RoutineJoba(joba.controller, joba.input, outputs, joba.method, joba.type));
      }
      else result.add(joba);
    }
    return result;
  }

  @Override
  public Joba[] buildVariants(Ref[] state, final JobExecutorService executor) {
    final MultiMap<MRPath, MRPath> map = new MultiMap<>();
    final Map<MRPath, BitSet> exists = new HashMap<>();
    for(int i = 0; i < state.length; i++) {
      if (MRPath.class.isAssignableFrom(state[i].type())) {
        final MRPath ref = (MRPath)state[i].resolve(executor);
        if (ref.mount == MRPath.Mount.TEMP) {
          final Matcher matcher = MERGE_PATTERN.matcher(ref.path);
          if (matcher.find()) {
            final MRPath resource = new MRPath(MRPath.Mount.valueOf(matcher.group(2)), matcher.group(1), false);
            map.put(resource, ref);
            BitSet bitSet = exists.get(resource);
            if (bitSet == null) {
              final int nbits = Integer.parseInt(matcher.group(3));
              exists.put(resource, bitSet = new BitSet(nbits));
              bitSet.set(0, nbits);
            }
            bitSet.clear(Integer.parseInt(matcher.group(4)));
          }
        }
      }
    }
    final Set<MRPath> keys = map.getKeys();
    final List<Joba> variants = new ArrayList<>();
    for (final MRPath resource : keys) {
      final Collection<MRPath> paths = map.get(resource);
      if (!exists.get(resource).isEmpty())
        continue;
      variants.add(new Joba.Stub(paths.toArray(new MRPath[paths.size()]), new MRPath[]{resource}) {
        @Override
        public void run() {
          executor.domain(MREnv.class).copy((MRPath[]) consumes(), resource, false);
        }

        public String toString() {
          return "Merge " + Arrays.toString(consumes()) + " -> " + resource;
        }
      });
    }
    return variants.toArray(new Joba[variants.size()]);
  }
}
