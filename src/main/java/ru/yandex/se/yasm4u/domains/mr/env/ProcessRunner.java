package ru.yandex.se.yasm4u.domains.mr.env;

import java.io.InputStream;
import java.util.List;

/**
 * User: solar
 * Date: 31.10.14
 * Time: 19:02
 */
public interface ProcessRunner {
  Process start(List<String> options, final InputStream input);
  Process start(String... options);
  Process start(InputStream input, String... options);
  void close();
}
