package com.expleague.yasm4u.domains.mr;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import com.expleague.commons.io.StreamTools;
import com.expleague.commons.seq.CharSeqTools;
import com.expleague.yasm4u.domains.mr.ops.impl.MRTableState;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.bytecode.ConstPool;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import sun.net.www.protocol.file.FileURLConnection;

/**
 * User: solar
 * Date: 24.09.14
 * Time: 9:34
 */
public class MRTools {
  public static final long TABLE_FRESHNESS_TIMEOUT = TimeUnit.DAYS.toMillis(30);
  public static final long DIR_FRESHNESS_TIMEOUT = TimeUnit.HOURS.toMillis(10);
  private static Logger LOG = Logger.getLogger(MRTools.class);
//  public static final String FORBIDEN = MREnv.class.getName().replace('.', '/');
  static {
    ClassPool.doPruning = true;
  }


  public static void buildClosureJar(final Class aRootClass, final String outputJar, Consumer<Class> action) throws IOException {
    buildClosureJar(aRootClass, outputJar, action, s -> false, Collections.emptyMap());
  }

  public static void buildClosureJar(final Class aRootClass, final String outputJar, Consumer<Class> action, Predicate<String> filter, final Map<String, byte[]> resourcesMap)
      throws IOException
  {
    final URLClassLoader parent = (URLClassLoader)aRootClass.getClassLoader();

    final Manifest manifest = new Manifest();
    final Attributes attributes = manifest.getMainAttributes();
    attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
    attributes.put(Attributes.Name.MAIN_CLASS, aRootClass.getName());

    try (final JarOutputStream file = new JarOutputStream(new FileOutputStream(outputJar), manifest)) {
      final Set<String> resources = new HashSet<>();
      final List<String> needLoading = new ArrayList<>();
      final Constructor<String> charArrConstructor = String.class.getConstructor(char[].class);

      final URLClassLoader classLoader = new ResourceCapturingURLClassLoader(parent, resources, filter, needLoading, resourcesMap, charArrConstructor);
      action.accept(classLoader.loadClass(aRootClass.getName()));
      while (!needLoading.isEmpty()) {
        //noinspection RedundantStringConstructorCall
        classLoader.loadClass(new String(needLoading.remove(0)));
      }

      for (Map.Entry<String, byte[]> entry : resourcesMap.entrySet()) {
        final String resourceName = entry.getKey();
        file.putNextEntry(new JarEntry(resourceName));
        file.write(entry.getValue());
        file.closeEntry();
      }

      for (Iterator<String> iterator = resources.iterator(); iterator.hasNext(); ) {
        final String resourceName = new String(iterator.next().toCharArray());
        final URL resource = parent.findResource(resourceName);
        if (resource != null) {
          file.putNextEntry(new JarEntry(resourceName));
          StreamTools.transferData(resource.openStream(), file);
          file.closeEntry();
        }
        else
          LOG.warn("Requested resource was not found in current classpath: " + resourceName);
      }
      file.close();
      classLoader.close();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      // never happen
    }
  }

  public static MRTableState updateTableShard(String path, boolean sorted, CounterInputStream cis) {
    return new MRTableState(path, true, sorted,
            "" + cis.totalLength(), cis.totalLength(), cis.keysCount(), cis.recordsCount(),
            System.currentTimeMillis(), System.currentTimeMillis());
  }

  public static class CounterInputStream extends InputStream {
    private final LineNumberReader delegate;
    private byte[] buffer;
    private int offset = 0;
    private long recordsCount;
    private long keysCount;
    private long totalLength;

    private final CharSequence[] result = new CharSequence[2];
    private CharSequence prevKey;

    public CounterInputStream(LineNumberReader delegate, long recordsCount, long keysCount, long totalLength) {
      this.delegate = delegate;
      this.recordsCount = recordsCount;
      this.keysCount = keysCount;
      this.totalLength = totalLength;
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
      if (!readNext())
        return -1;
      final int toCopy = Math.min(len, buffer.length - offset);
      System.arraycopy(buffer, offset, b, off, toCopy);
      offset += toCopy;
      return toCopy;
    }

    @Override
    public int read() throws IOException {
      if (!readNext())
        return -1;
      return buffer[offset++];
    }

    public long recordsCount() {
      return recordsCount;
    }

    public long keysCount() {
      return keysCount;
    }

    public long totalLength() {
      return totalLength;
    }

    private boolean readNext() throws IOException {
      if (buffer != null && offset < buffer.length)
        return true;
      final String line = delegate.readLine();
      if (line == null)
        return false;
      CharSeqTools.split(line, '\t', result);
      if (!result[0].equals(prevKey))
        keysCount ++;
      prevKey = result[0];
      recordsCount++;
      buffer = (line + "\n").getBytes(StreamTools.UTF);
      offset = 0;
      totalLength += buffer.length;
      return true;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static class ResourceCapturingURLClassLoader extends URLClassLoader {
    private final Set<String> known = new HashSet<>();
    private final ClassPool pool = new ClassPool();
    private final Set<String> resources;
    private final Predicate<String> filter;
    private final List<String> needLoading;
    private final Map<String, byte[]> resourcesMap;
    private final Constructor<String> charArrConstructor;
    private boolean kosherResource;

    public ResourceCapturingURLClassLoader(URLClassLoader parent, Set<String> resources, Predicate<String> filter, List<String> needLoading, Map<String, byte[]> resourcesMap, Constructor<String> charArrConstructor) {
      super(parent.getURLs(), null);
      this.resources = resources;
      this.filter = filter;
      this.needLoading = needLoading;
      this.resourcesMap = resourcesMap;
      this.charArrConstructor = charArrConstructor;
      kosherResource = false;
    }

    @Override
    public synchronized Class<?> loadClass(final String name) throws ClassNotFoundException {
      try {
        known.add(name);
        if (!resources.contains(name) && filter.test(name)) {
          final URL resource = getResource(name.replace('.', '/').concat(".class"));
          if (kosherResource) {
            assert resource != null;
            final CtClass ctClass = pool.makeClass(resource.openStream());
            final ConstPool constPool = ctClass.getClassFile().getConstPool();
            final Set classNames = constPool.getClassNames();
            for (Iterator iterator = classNames.iterator(); iterator.hasNext(); ) {
              String next = (String) iterator.next();
              if (next.startsWith("[L"))
                next = next.substring(2);
              else if (next.startsWith("[")) // primitive arrays
                continue;
              if (next.endsWith(";"))
                next = next.substring(0, next.length() - 1);
              next = next.replace('/', '.');
              if (!known.contains(next)) {
//                    System.err.println(next);
                needLoading.add(next);
              }
              known.add(next);
            }
            ctClass.detach();
            ctClass.prune();
          }
        }
      } catch (IOException e) {
        // skip
      }
      return super.loadClass(name);
    }

    @Override
    public synchronized URL getResource(final String name) {
      try {
        @SuppressWarnings("PrimitiveArrayArgumentToVariableArgMethod")
        final byte[] content = resourcesMap.get(charArrConstructor.newInstance(name.toCharArray()));
        if (content != null) {
          return new URL("jar", "localhost", -1, "/" + name, new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(final URL u) throws IOException {
              return new FileURLConnection(u, new File("/dev/null")) {
                @Override
                public long getContentLengthLong() {
                  return content.length;
                }

                @Override
                public synchronized InputStream getInputStream() {
                  return new ByteArrayInputStream(content);
                }
              };
            }
          });
        }
        final URL resource = super.getResource(name);
        kosherResource = resource != null;// && !name.contains(FORBIDEN);
        if (kosherResource && "jar".equals(resource.getProtocol())) {
          final JarURLConnection connection;
          connection = (JarURLConnection) resource.openConnection();
          if ("rt.jar".equals(new File(connection.getJarFileURL().getFile()).getName()))
            kosherResource = false;
        }
        if (kosherResource) {
          LOG.debug("Resource: " + name);
          resources.add(name);
        }
        return resource;
      } catch (IOException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
