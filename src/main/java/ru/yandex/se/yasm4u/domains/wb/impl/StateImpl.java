package ru.yandex.se.yasm4u.domains.wb.impl;

import com.spbsu.commons.func.Computable;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.util.ArrayTools;
import org.jetbrains.annotations.Nullable;
import ru.yandex.se.yasm4u.Domain;
import ru.yandex.se.yasm4u.Joba;
import ru.yandex.se.yasm4u.Ref;
import ru.yandex.se.yasm4u.Routine;
import ru.yandex.se.yasm4u.domains.mr.MREnv;
import ru.yandex.se.yasm4u.domains.wb.State;
import ru.yandex.se.yasm4u.domains.wb.StateRef;
import ru.yandex.se.yasm4u.domains.wb.TempRef;
import ru.yandex.se.yasm4u.impl.JobExecutorServiceBase;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class StateImpl implements State {
  // never ever use this variable out of impl package!!!!!!!!
  protected Map<StateRef, Object> state = new HashMap<>();
  protected Domain.Controller trash;


  public StateImpl() {
    trash = new JobExecutorServiceBase(this) {
      @Override
      public Future<List<?>> calculate(Ref<?, ?>... goals) {
        return null;
      }
    };
  }

  public StateImpl(MREnv env) {
    trash = new JobExecutorServiceBase(this, env) {
      @Override
      public Future<List<?>> calculate(Ref<?, ?>... goals) {
        return null;
      }
    };
  }

  public StateImpl(State copy, Controller trash) {
    this.trash = trash;
    for (final StateRef key : copy.keys()) {
      state.put(key, copy.get(key));
    }
  }

  @Nullable
  @Override
  public <T> T get(final String name) {
    //noinspection unchecked
    return get((StateRef<? extends T>) trash.parse(name));
  }

  @Nullable
  @Override
  public <T> T get(final StateRef<T> name) {
    //noinspection unchecked
    if (!state.containsKey(name))
      return null;

    //noinspection unchecked
    return (T)state.get(name);
  }

  @Override
  public boolean available(final String... consumes) {
    return available(ArrayTools.map(consumes, StateRef.class, new Computable<String, StateRef>() {
      @Override
      public StateRef compute(String argument) {
        return (StateRef)trash.parse(argument);
      }
    }));
  }

  @Override
  public boolean available(StateRef... consumes) {
    for (int i = 0; i < consumes.length; i++) {
      if (!state.containsKey(consumes[i]))
        return false;
    }
    return true;
  }

  @Override
  public Set<? extends StateRef> keys() {
    return state.keySet();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    final SerializationRepository<CharSequence> serialization = State.SERIALIZATION;
    for(final StateRef current : keys()) {
      final Object instance = get(current);
      assert instance != null;
      final Class conversionType = serialization.base.conversionType(instance.getClass(), CharSequence.class);
      out.writeBoolean(true);
      out.writeUTF(current.toString());
      out.writeUTF(conversionType.getName());
      out.writeUTF(serialization.write(instance).toString());
    }
    out.writeBoolean(false);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    state = new HashMap<>();
    trash = new JobExecutorServiceBase(this){ // no MREnv
      @Override
      public Future<List<?>> calculate(Ref<?, ?>... goals) {
        return null;
      }
    };

    while(in.readBoolean()) {
      final StateRef current = (StateRef)trash.parse(in.readUTF());
      final String itemClass = in.readUTF();
      final Object read = State.SERIALIZATION.read(in.readUTF(), Class.forName(itemClass));
      state.put(current, read);
    }
  }

  private void readObjectNoData() throws ObjectStreamException {
    state = new HashMap<>();
  }

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {
    jobs.add(new PublisherJoba(this));
  }

  @Override
  public void publishReferenceParsers(Ref.Parser parser, final Controller controller) {
    parser.registerProtocol("var", new TypeConverter<String, StateRef<?>>() {
      @Override
      public StateRef<?> convert(final String from) {
        //noinspection unchecked
        return new StateRef(from, Object.class);
      }
    });
    parser.registerProtocol("temp", new TypeConverter<String, TempRef<?>>() {
      @Override
      public TempRef<?> convert(String from) {
        return new TempRef<>(from, Object.class, controller);
      }
    });
  }

  public <T extends StateRef> T parseRef(CharSequence from) {
    //noinspection unchecked
    return (T)trash.parse(from);
  }
}
