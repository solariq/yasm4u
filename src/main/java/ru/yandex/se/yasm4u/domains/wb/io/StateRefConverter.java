package ru.yandex.se.yasm4u.domains.wb.io;

import com.spbsu.commons.func.Converter;
import ru.yandex.se.yasm4u.domains.wb.StateRef;
import ru.yandex.se.yasm4u.domains.wb.impl.StateImpl;

/**
 * User: solar
 * Date: 06.04.15
 * Time: 17:48
 */
public class StateRefConverter implements Converter<StateRef, CharSequence> {
  @Override
  public CharSequence convertTo(StateRef object) {
    return object.toURI().toString();
  }

  @Override
  public StateRef convertFrom(CharSequence source) {
    return new StateImpl().parseRef(source);
  }
}
