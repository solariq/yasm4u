package solar.mr.proc.impl;

import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.spbsu.commons.filters.ClassFilter;
import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.TypeConverter;
import solar.mr.proc.MRState;
import solar.mr.proc.MRWhiteboard;
import solar.mr.tables.MRTableShard;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class MRStateImpl implements MRState, Serializable {
  // never ever use this variable out of impl package!!!!!!!!
  Map<String, Object> state = new HashMap<>();

  public MRStateImpl() {}

  public MRStateImpl(MRState copy) {
    for (int i = 0; i < copy.available().length; i++) {
      final String key = copy.available()[i];
      state.put(key, copy.get(key));
    }
  }

  @Nullable
  @Override
  public <T> T get(final String uri) {
    //noinspection unchecked
    return (T)state.get(resolveVars(uri));
  }

  private static final Pattern varPattern = Pattern.compile("\\{([^\\},]+)([^\\}]+)\\}");
  String resolveVars(String resource) {
    final Matcher matcher = varPattern.matcher(resource);
    final StringBuffer format = new StringBuffer();
    final Map<String, Integer> namesMap = new HashMap<>();
    while(matcher.find()) {
      final String name = matcher.group(1);
      final int index = namesMap.containsKey(name) ? namesMap.get(name) : namesMap.size();
      namesMap.put(name, index);
      matcher.appendReplacement(format, "{" + index + (matcher.groupCount() > 1 ? matcher.group(2) : "") + "}");
    }
    matcher.appendTail(format);
    final Object[] args = new Object[namesMap.size()];
    for (final Map.Entry<String, Integer> entry : namesMap.entrySet()) {
      args[entry.getValue()] = get(entry.getKey());
    }
    return MessageFormat.format(format.toString(), args);
  }

  @Override
  public boolean available(final String... consumes) {
    for (int i = 0; i < consumes.length; i++) {
      if (!available(consumes[i]))
        return false;
    }
    return true;
  }

  private boolean available(final String consumes) {
    final String key = resolveVars(consumes);
    if (!state.containsKey(key))
      return false;
    final Object resource = state.get(key);
    if (resource instanceof MRTableShard)
      return ((MRTableShard) resource).refresh().isAvailable();
    return true;
  }

  @Override
  public String[] available() {
    final String[] available = new String[state.size()];
    int index = 0;
    for (String key : state.keySet()) {
      available[index++] = key;
    }
    return available;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    for(int i = 0; i < available().length; i++) {
      final String current = available()[i];
      final Object instance = get(current);
      assert instance != null;
      final SerializationRepository<CharSequence> serialization = MRState.SERIALIZATION;
      final TypeConverter<CharSequence, ?> converter = serialization.base.converter(CharSequence.class, instance.getClass());
      if (converter != null && !new ClassFilter<TypeConverter>(Action.class, MRWhiteboard.class).accept(converter)) {
        out.writeBoolean(true);
        out.writeUTF(current);
        out.writeUTF(instance.getClass().getName());
        out.writeUTF(serialization.write(instance).toString());
      }
    }
    out.writeBoolean(false);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    while(in.readBoolean()) {
      final String current = in.readUTF();
      final String itemClass = in.readUTF();
      final Object read = MRState.SERIALIZATION.read(in.readUTF(), Class.forName(itemClass));
      if (state == null)
        state = new HashMap<>();
      state.put(current, read);
    }
  }

  private void readObjectNoData() throws ObjectStreamException {
  }
}
