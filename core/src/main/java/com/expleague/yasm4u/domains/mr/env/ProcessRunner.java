package com.expleague.yasm4u.domains.mr.env;

import java.io.InputStream;
import java.util.*;

/**
 * User: solar
 * Date: 31.10.14
 * Time: 19:02
 */
public interface ProcessRunner {
  Process start(final List<String> options, final Set<String> files, final InputStream input);
  void close();

  default void addOptionWithFile(String opt) {
  }

  default Process start(String... options) {
    return start(Arrays.asList(options), null);
  }

  default Process start(InputStream input, String... options) {
    return start(Arrays.asList(options), input);
  }

  default Process start(final List<String> options, final InputStream input) {
    return start(options, Collections.emptySet(), input);
  }
}
