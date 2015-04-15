package ru.yandex.se.lyadzhin.report.viewports;

import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;

import java.util.Arrays;


/**
 * User: lyadzhin
 * Date: 10.04.15 23:46
 */
public class ViewportRequestingJoba implements Joba {
  private final ViewportBuilder viewportBuilder;

  public ViewportRequestingJoba(ViewportBuilder viewportBuilder) {
    this.viewportBuilder = viewportBuilder;
  }

  @Override
  public Ref[] consumes() {
    return new Ref[0];
  }

  @Override
  public Ref[] produces() {
    return viewportBuilder.requests();
  }

  @Override
  public void run() {
    System.out.println("Publishing requests for viewport " + viewportBuilder.id() + ": " +
            Arrays.toString(viewportBuilder.requests()));
  }
}
