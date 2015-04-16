package ru.yandex.se.yasm4u;

import java.util.Arrays;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:20
 */
public interface Joba extends Runnable {
  Ref[] consumes();
  Ref[] produces();

  abstract class Stub implements Joba {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Stub)) return false;
      final Stub stub = (Stub) o;
      return Arrays.equals(consumes(), stub.consumes()) && Arrays.equals(produces(), stub.produces());

    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(produces());
      result = 31 * result + Arrays.hashCode(consumes());
      return result;
    }
  }
}
