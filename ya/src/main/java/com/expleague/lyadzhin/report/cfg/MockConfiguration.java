package com.expleague.lyadzhin.report.cfg;

import java.util.Arrays;
import java.util.List;

/**
* User: lyadzhin
* Date: 11.04.15 0:08
*/
class MockConfiguration implements Configuration {
  @Override
  public List<String> viewportIdList() {
    return Arrays.asList("web", "images", "movies");
  }
}
