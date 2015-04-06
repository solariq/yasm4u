package ru.yandex.se.yasm4u;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:20
 */
public interface Joba extends Runnable {
  Ref[] consumes();
  Ref[] produces();

  abstract class Stub implements Joba {
    private final Ref[] produces;
    private final Ref[] consumes;

    protected Stub(Ref<?, ?>[] consumes, Ref<?, ?>[] produces) {
      this.produces = produces;
      this.consumes = consumes;
    }

    @Override
    public final Ref[] consumes() {
      return consumes;
    }

    @Override
    public final Ref[] produces() {
      return produces;
    }
  }
}
