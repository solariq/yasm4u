package com.expleague.mobile.logprocessing.core;

import com.expleague.commons.seq.CharSeqTools;
import com.expleague.commons.util.Pair;
import com.expleague.yasm4u.domains.mr.MREnv;
import com.expleague.yasm4u.domains.mr.MROutput;
import com.expleague.yasm4u.domains.mr.env.SSHProcessRunner;
import com.expleague.yasm4u.domains.mr.env.YaMREnv;
import com.expleague.yasm4u.domains.mr.routines.ann.AnnotatedMRProcess;
import com.expleague.yasm4u.domains.mr.routines.ann.tags.MRMapMethod;
import com.expleague.yasm4u.domains.mr.routines.ann.tags.MRProcessClass;
import com.expleague.yasm4u.domains.mr.routines.ann.tags.MRReduceMethod;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.wb.Whiteboard;
import com.expleague.yasm4u.domains.mr.env.ProcessRunner;
import com.expleague.yasm4u.domains.wb.impl.WhiteboardImpl;

import java.util.Iterator;

/**
 * User: inikifor
 * Date: 05.11.14
 * Time: 15:11
 */
public final class Main {
  @SuppressWarnings("UnusedDeclaration")
  @MRProcessClass(goal = "mr:///mobilesearchtest/count_tmp")
  public static final class SampleCounter {

    private static final String TMP_TABLE_URI = "temp:mr:///test-counter";

    private final State state;

    public SampleCounter(State state) {
      this.state = state;
    }

    @MRMapMethod(input = "mr:///mobilesearchtest/20141017_655_11", output = TMP_TABLE_URI)
    public void map(final String key, final String sub, final CharSequence value, MROutput output) {
      int len = value.length();
      switch (len % 3) {
        case 0:
          output.add("% 3 = 0", "" + len, "1");
          break;
        case 1:
          output.add("% 3 = 1", "" + len, "1");
          break;
        case 2:
          output.add("% 3 = 2", "" + len, "1");
          break;
      }
    }

    @MRReduceMethod(input = TMP_TABLE_URI, output = "mr:///mobilesearchtest/count_tmp")
    public void reduce(final String key, final Iterator<Pair<String, CharSequence>> reduce, MROutput output) {
      int count = 0;
      while (reduce.hasNext()) {
        Pair<String, CharSequence> sv = reduce.next();
        count += CharSeqTools.parseInt(sv.second);
      }
      output.add(key, "1", "" + count);
    }

  }

  public static void main(String[] args) {
    System.out.println("Starting Java MR calculation!");
    final ProcessRunner runner = new SSHProcessRunner("batista", "/Berkanavt/mapreduce/bin/mapreduce-dev");
    final MREnv env = new YaMREnv(runner, "mobilesearch", "cedar:8013");
    final Whiteboard wb = new WhiteboardImpl(env, SampleCounter.class.getName());
    final AnnotatedMRProcess mrProcess = new AnnotatedMRProcess(SampleCounter.class, wb, env);
    mrProcess.execute();
    mrProcess.wb().wipe();
  }

}
