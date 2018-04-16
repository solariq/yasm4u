package com.expleague.yasm4u.domains.wb.impl;

import com.expleague.commons.func.types.SerializationRepository;
import com.expleague.commons.func.types.TypeConverter;
import com.expleague.commons.util.ArrayTools;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.wb.TempRef;
import org.jetbrains.annotations.Nullable;
import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.domains.mr.MREnv;
import com.expleague.yasm4u.domains.wb.State;
import com.expleague.yasm4u.domains.wb.StateRef;
import com.expleague.yasm4u.impl.JobExecutorServiceBase;

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
@SuppressWarnings("WeakerAccess")
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
      //noinspection unchecked
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
    //noinspection unchecked
    return available(ArrayTools.map(consumes, StateRef.class, trash::parse));
  }

  @Override
  public boolean available(StateRef... consumes) {
    for (StateRef consume : consumes) {
      if (!state.containsKey(consume))
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
      //noinspection unchecked
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
      //noinspection unchecked
      final StateRef current = trash.parse(in.readUTF());
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
    parser.registerProtocol("var", (TypeConverter<String, StateRef<?>>) from -> {
      //noinspection unchecked
      return new StateRef(from, Object.class);
    });
    parser.registerProtocol("temp", (TypeConverter<String, TempRef<?>>) from -> new TempRef<>(from, Object.class, controller));
  }

  public <T extends StateRef> T parseRef(CharSequence from) {
    //noinspection unchecked
    return (T)trash.parse(from);
  }
}
