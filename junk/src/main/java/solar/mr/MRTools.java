package solar.mr;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;


import com.spbsu.commons.func.Action;
import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.io.StreamTools;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.bytecode.ConstPool;
import org.apache.log4j.Logger;
import sun.net.www.protocol.file.FileURLConnection;

/**
 * User: solar
 * Date: 24.09.14
 * Time: 9:34
 */
public class MRTools {
  private static Logger LOG = Logger.getLogger(MRTools.class);
  public static final String FORBIDEN = MREnv.class.getName().replace('.', '/');

  public static void buildClosureJar(final Class aRootClass, final String outputJar, Action<Class> action) throws IOException {
    buildClosureJar(aRootClass, outputJar, action, Collections.<String,byte[]>emptyMap());
  }

  public static void buildClosureJar(final Class aRootClass, final String outputJar, Action<Class> action, final Map<String, byte[]> resourcesMap)
      throws IOException
  {
    final URLClassLoader parent = (URLClassLoader)aRootClass.getClassLoader();

    final Manifest manifest = new Manifest();
    final Attributes attributes = manifest.getMainAttributes();
    attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
    attributes.put(Attributes.Name.MAIN_CLASS, aRootClass.getName());

    try (final JarOutputStream file = new JarOutputStream(new FileOutputStream(outputJar), manifest)) {
      final Set<String> resources = new HashSet<>();
      final Set<String> needLoading = new HashSet<>();
      final Constructor<String> charArrConstructor = String.class.getConstructor(char[].class);

      final URLClassLoader classLoader = new URLClassLoader(parent.getURLs(), null) {
        private final Set<String> known = new HashSet<>();
        private final ClassPool pool = new ClassPool();
        private boolean kosherResource = false;
        @Override
        public synchronized Class<?> loadClass(final String name) throws ClassNotFoundException {
          try {
            known.add(name);
            if (!resources.contains(name)) {
              needLoading.remove(name);
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
//                    System.out.println(next);
                    needLoading.add(next);
                  }
                  known.add(next);
                }
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
                    public synchronized InputStream getInputStream() throws IOException {
                      return new ByteArrayInputStream(content);
                    }
                  };
                }
              });
            }
            final URL resource = super.getResource(name);
            kosherResource = resource != null && !name.contains(FORBIDEN);
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
      };
      action.invoke(classLoader.loadClass(aRootClass.getName()));
      while (!needLoading.isEmpty()) {
        //noinspection RedundantStringConstructorCall
        classLoader.loadClass(new String(needLoading.iterator().next()));
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
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      // never happen
    }
  }
}
