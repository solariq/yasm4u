package solar.mr.proc.impl;

import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import solar.mr.proc.MRState;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public class MRStateImpl implements MRState, Serializable {
  // never ever use this variable out of impl package!!!!!!!!
  final Map<String, Object> state = new HashMap<>();

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
    return (T)state.get(uri);
  }

  private final Pattern varPattern = Pattern.compile("\\{([^\\},]+)(,[^\\},]+)*\\}");
  String resolveVars(String resource) {
    final Matcher matcher = varPattern.matcher(resource);
    final StringBuffer format = new StringBuffer();
    final Map<String, Integer> namesMap = new HashMap<>();
    while(matcher.find()) {
      final String name = matcher.group(1);
      final int index = namesMap.containsKey(name) ? namesMap.get(name) : namesMap.size();
      namesMap.put(name, index);
      matcher.appendReplacement(format, "{" + index + "}");
    }
    matcher.appendTail(format);
    final Object[] args = new Object[namesMap.size()];
    for (final Map.Entry<String, Integer> entry : namesMap.entrySet()) {
      args[entry.getValue()] = resolveVars(entry.getKey());
    }
    return MessageFormat.format(format.toString(), args);
  }

  @Override
  public boolean availableAll(final String[] consumes) {
    for (int i = 0; i < consumes.length; i++) {
      if (!available(consumes[i]))
        return false;
    }
    return true;
  }

  @Override
  public boolean available(final String consumes) {
    return state.containsKey(consumes);
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
    out.writeInt(available().length);
    for(int i = 0; i < available().length; i++) {
      final String current = available()[i];
      final Object instance = get(current);
      out.writeUTF(current);
      assert instance != null;
      out.writeUTF(instance.getClass().getName());
      out.writeUTF(MRState.SERIALIZATION.write(instance).toString());
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    final int count = in.readInt();
    for (int i = 0; i < count; i++) {
      final String current = in.readUTF();
      final String itemClass = in.readUTF();
      final Object read = MRState.SERIALIZATION.read(in.readUTF(), Class.forName(itemClass));
      state.put(current, read);
    }
  }

  private void readObjectNoData() throws ObjectStreamException {
  }
}
