/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jbrisbin.vpc.zk;

import groovy.lang.Closure;
import groovy.lang.ExpandoMetaClass;
import groovy.lang.GString;
import groovy.lang.GroovyObjectSupport;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Helper object for making working with ZooKeeper a little more pleasant. With this helper, you can do stuff like:
 * <p/>
 * <pre><code>zk = new GroovyZooKeeperHelper("localhost:2181",
 *   onNodeChildrenChanged: { evt ->
 *     println "childrenChanged: ${evt}"
 *   },
 *   onDataChanged: { evt ->
 *     println "data changed: ${evt}"
 *   },
 *   onEvent: { evt ->
 *     println "none: ${evt}"
 *   }
 * )
 * if (!zk.exists("/my/node/parent")) {
 *   zk.createPersistentNodeAndParents("/my/node/parent")
 * }
 * node = zk.createPersistentNode("/my/node/parent/child")
 * node.data = 12345
 * </code></pre>
 *
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class GroovyZooKeeperHelper extends GroovyObjectSupport {

  protected static final Logger log = LoggerFactory.getLogger(GroovyZooKeeperHelper.class);

  /**
   * The internal ZooKeeper object.
   */
  private ZooKeeper zookeeper;
  /**
   * Generic {@link org.apache.zookeeper.Watcher} wrapper that delegates event callbacks to various user-defined
   * closures.
   */
  private Watcher clientWatcher;
  /**
   * {@link groovy.lang.Closure} for the NodeChildrenChanged event.
   */
  private Closure onNodeChildrenChanged;
  /**
   * {@link groovy.lang.Closure} for the NodeCreated event.
   */
  private Closure onNodeCreated;
  /**
   * {@link groovy.lang.Closure} for the NodeDeleted event.
   */
  private Closure onNodeDeleted;
  /**
   * {@link groovy.lang.Closure} for the DataChanged event.
   */
  private Closure onDataChanged;
  /**
   * {@link groovy.lang.Closure} for the None event (the default if others aren't defined).
   */
  private Closure onEvent;
  private int timeout = 20000;

  /**
   * Create an empty helper that has no {@link org.apache.zookeeper.ZooKeeper} delegate.
   */
  public GroovyZooKeeperHelper() {
    setMetaClass(new ExpandoMetaClass(getClass()));
    getMetaClass().initialize();
    clientWatcher = new ClientWatcher();
  }

  /**
   * Create this helper with the specified {@link org.apache.zookeeper.ZooKeeper} delegate.
   *
   * @param zookeeper The {@link org.apache.zookeeper.ZooKeeper} delegate
   */
  public GroovyZooKeeperHelper(ZooKeeper zookeeper) {
    this();
    this.zookeeper = zookeeper;
  }

  /**
   * Create this helper and {@link org.apache.zookeeper.ZooKeeper} delegate connected to the specified URL string.
   *
   * @param url The URL to connect to (e.g. "localhost:2181")
   * @throws IOException
   */
  public GroovyZooKeeperHelper(String url) throws IOException {
    this();
    setZookeeper(new ZooKeeper(url, timeout, clientWatcher));
  }

  public GroovyZooKeeperHelper(String url, int timeout) throws IOException {
    this();
    this.timeout = timeout;
    setZookeeper(new ZooKeeper(url, timeout, clientWatcher));
  }

  /**
   * Create this helper and {@link org.apache.zookeeper.ZooKeeper} delegate connected to the specified URL string, but
   * also set various callback {@link groovy.lang.Closure}s at once.
   *
   * @param callbacks
   * @param url
   * @throws IOException
   */
  public GroovyZooKeeperHelper(Map<String, Closure> callbacks, String url) throws IOException {
    this();
    for (String key : callbacks.keySet()) {
      try {
        getClass().getDeclaredField(key).set(this, callbacks.get(key));
      } catch (IllegalAccessException e) {
        log.error(e.getMessage(), e);
      } catch (NoSuchFieldException e) {
        log.error(e.getMessage(), e);
      }
    }
    setZookeeper(new ZooKeeper(url, timeout, clientWatcher));
  }

  /**
   * Access the internal {@see org.apache.zookeeper.ZooKeeper} delegate.
   *
   * @return
   */
  public ZooKeeper getZookeeper() {
    return zookeeper;
  }

  /**
   * Set the internal {@see org.apache.zookeeper.ZooKeeper} delegate. Also implicitly closes any delegate that was
   * previously set.
   *
   * @param zookeeper
   */
  public void setZookeeper(ZooKeeper zookeeper) {
    if (null != this.zookeeper) {
      try {
        this.zookeeper.close();
      } catch (InterruptedException e) {
        log.error(e.getMessage(), e);
      }
    }
    this.zookeeper = zookeeper;
  }

  /**
   * Access the {@see org.apache.zookeeper.Watcher} that delegates to user-defined {@see groovy.lang.Closure}s.
   *
   * @return
   */
  public Watcher getClientWatcher() {
    return clientWatcher;
  }

  /**
   * Set the {@see org.apache.zookeeper.Watcher} that delegates to user-defined {@see groovy.lang.Closure}s.
   *
   * @param clientWatcher
   */
  public void setClientWatcher(Watcher clientWatcher) {
    this.clientWatcher = clientWatcher;
    if (null != this.zookeeper) {
      this.zookeeper.register(this.clientWatcher);
    }
  }

  public Closure getOnNodeChildrenChanged() {
    return onNodeChildrenChanged;
  }

  public void setOnNodeChildrenChanged(Closure onNodeChildrenChanged) {
    this.onNodeChildrenChanged = onNodeChildrenChanged;
  }

  public Closure getOnNodeCreated() {
    return onNodeCreated;
  }

  public void setOnNodeCreated(Closure onNodeCreated) {
    this.onNodeCreated = onNodeCreated;
  }

  public Closure getOnNodeDeleted() {
    return onNodeDeleted;
  }

  public void setOnNodeDeleted(Closure onNodeDeleted) {
    this.onNodeDeleted = onNodeDeleted;
  }

  public Closure getOnDataChanged() {
    return onDataChanged;
  }

  public void setOnDataChanged(Closure onDataChanged) {
    this.onDataChanged = onDataChanged;
  }

  public Closure getOnEvent() {
    return onEvent;
  }

  public void setOnEvent(Closure onEvent) {
    this.onEvent = onEvent;
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /**
   * Close all resources.
   *
   * @throws InterruptedException
   */
  public void close() throws InterruptedException {
    zookeeper.close();
  }

  /**
   * Delete any node version at the given path.
   *
   * @param path plain or Groovy string
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void delete(Object path) throws InterruptedException, KeeperException {
    delete(path, -1);
  }

  /**
   * Delete the node with the given version.
   *
   * @param path    plain or Groovy string
   * @param version version to delete
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void delete(Object path, int version) throws InterruptedException, KeeperException {
    zookeeper.delete(getPathAsString(path), version);
  }

  /**
   * Asynchronously delete the node at the given path, with the specified version, and execute the given {@link
   * groovy.lang.Closure} as a callback.
   *
   * @param path
   * @param version
   * @param callback
   */
  public void delete(Object path, int version, final Closure callback) {
    zookeeper.delete(getPathAsString(path), version, new AsyncCallback.VoidCallback() {
      public void processResult(int rc, String path, Object ctx) {
        callback.setProperty("returnCode", rc);
        callback.setProperty("path", path);
        callback.setDelegate(ctx);
        callback.call();
      }
    }, this);
  }

  /**
   * Create a EPHEMERAL_SEQUENTIAL type node.
   *
   * @param path
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Node createSequenceNode(Object path) throws InterruptedException, KeeperException {
    return create(path, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  /**
   * Create a PERSISTENT type node.
   *
   * @param path
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Node createPersistentNode(Object path) throws InterruptedException, KeeperException {
    return create(path, CreateMode.PERSISTENT);
  }

  /**
   * Create a PERSISTENT type node, but also create any intermediate parent nodes.
   *
   * @param path
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public String createPersistentNodeAndParents(
      Object path) throws InterruptedException, KeeperException {
    String spath = getPathAsString(path);
    String[] parts = spath.substring(1).split("/");
    StringBuffer buff = new StringBuffer();
    String fullPath = null;
    for (String p : parts) {
      buff.append("/").append(p);
      try {
        fullPath = zookeeper.create(buff.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
      } catch (KeeperException.NodeExistsException ignored) {
        fullPath = buff.toString();
      }
    }
    return fullPath;
  }

  /**
   * Create an EPHEMERAL type node (the default).
   *
   * @param path
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Node create(Object path) throws InterruptedException, KeeperException {
    return create(path, new byte[0]);
  }

  /**
   * Create a node of the given type.
   *
   * @param path
   * @param mode {@see org.apache.zookeeper.CreateMode}
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Node create(Object path,
                     CreateMode mode) throws InterruptedException, KeeperException {
    return create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
  }

  /**
   * Create an EPHEMERAL node with the given data.
   *
   * @param path
   * @param data
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Node create(Object path, Object data) throws InterruptedException, KeeperException {
    return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
  }

  /**
   * Create an EPHEMERAL node with the given data and call the given {@link groovy.lang.Closure} as callback.
   *
   * @param path
   * @param data
   * @param callback
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void create(Object path, Object data,
                     Closure callback) throws InterruptedException, KeeperException {
    create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, callback);
  }

  /**
   * Create a ZooKeeper node asynchronously by specifying all parameters.
   *
   * @param path
   * @param data
   * @param acls
   * @param mode
   * @param callback
   */
  public void create(Object path, Object data, List<ACL> acls, CreateMode mode,
                     final Closure callback) {
    zookeeper.create(getPathAsString(path), serialize(data), acls, mode,
        new AsyncCallback.StringCallback() {
          public void processResult(int rc, String path, Object ctx, String name) {
            callback.setProperty("returnCode", rc);
            callback.setProperty("path", path);
            callback.setDelegate(ctx);
            callback.call(name);
          }
        }, this);
  }

  /**
   * Create a ZooKeeper node synchronously by specifying all parameters.
   *
   * @param path
   * @param data
   * @param acls
   * @param mode
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Node create(Object path, Object data, List<ACL> acls, CreateMode mode) throws
      InterruptedException,
      KeeperException {
    String s = zookeeper.create(getPathAsString(path), serialize(data), acls, mode);
    return new Node(s, mode);
  }

  /**
   * Check if node exists and default to NOT setting a watch.
   *
   * @param path
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Stat exists(Object path) throws InterruptedException, KeeperException {
    return exists(path, false);
  }

  /**
   * Check if node exists asynchronously.
   *
   * @param path
   * @param watch
   * @param callback
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void exists(Object path, boolean watch,
                     final Closure callback) throws InterruptedException, KeeperException {
    zookeeper.exists(getPathAsString(path), watch, new AsyncCallback.StatCallback() {
      public void processResult(int rc, String path, Object ctx, Stat stat) {
        callback.setProperty("returnCode", rc);
        callback.setProperty("path", path);
        callback.setDelegate(ctx);
        callback.call(stat);
      }
    }, this);
  }

  /**
   * Check if a node exists synchronously.
   *
   * @param path
   * @param watch
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Stat exists(Object path, boolean watch) throws InterruptedException, KeeperException {
    return zookeeper.exists(getPathAsString(path), watch);
  }

  /**
   * Get a node's childern.
   *
   * @param path
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public List<String> getChildren(Object path) throws InterruptedException, KeeperException {
    return getChildren(path, false);
  }

  /**
   * Get a node's children and specify whether to set a watch or not.
   *
   * @param path
   * @param watch
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public List<String> getChildren(Object path,
                                  boolean watch) throws InterruptedException, KeeperException {
    return zookeeper.getChildren(getPathAsString(path), watch);
  }

  /**
   * Get a node's latest version of data.
   *
   * @param path
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Object getData(Object path) throws InterruptedException, KeeperException {
    return getData(path, null);
  }

  /**
   * Get a specific version of the node's data.
   *
   * @param path
   * @param stat
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Object getData(Object path, Stat stat) throws InterruptedException, KeeperException {
    return deserialize(zookeeper.getData(getPathAsString(path), false, stat));
  }

  /**
   * Get a node's data and set a watch.
   *
   * @param path
   * @param watcher
   * @param stat
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Object getData(Object path, final Closure watcher,
                        Stat stat) throws InterruptedException, KeeperException {
    return deserialize(zookeeper.getData(getPathAsString(path), new Watcher() {
      public void process(WatchedEvent event) {
        watcher.call(event);
      }
    }, stat));
  }

  /**
   * Set a node's data.
   *
   * @param path
   * @param data
   * @param version
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public Stat setData(Object path, Object data,
                      int version) throws InterruptedException, KeeperException {
    return zookeeper.setData(getPathAsString(path), serialize(data), version);
  }

  /**
   * Set a node's data asynchronously.
   *
   * @param path
   * @param data
   * @param version
   * @param callback
   */
  public void setData(Object path, Object data, int version, final Closure callback) {
    zookeeper.setData(getPathAsString(path), serialize(data), version,
        new AsyncCallback.StatCallback() {
          public void processResult(int rc, String path, Object ctx, Stat stat) {
            callback.setDelegate(ctx);
            callback.setProperty("returnCode", rc);
            callback.setProperty("path", path);
            callback.call(stat);
          }
        }, this);
  }

  /**
   * Turn a path into a plain String.
   *
   * @param path
   * @return
   */
  private String getPathAsString(Object path) {
    String spath = (path instanceof String || path instanceof GString ? path.toString() : null);
    if (null == spath) {
      throw new IllegalArgumentException("First parameter must be a String " + path);
    }
    return spath;
  }

  /**
   * Serialize an object to a byte array.
   *
   * @param obj
   * @return
   */
  private byte[] serialize(Object obj) {
    byte[] bytes = new byte[0];
    if (null != obj) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oout = null;
      try {
        oout = new ObjectOutputStream(out);
        oout.writeObject(obj);
      } catch (EOFException eof) {
        return new byte[0];
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
      bytes = out.toByteArray();
    }
    return bytes;
  }

  /**
   * Deserialize an object from a byte array.
   *
   * @param bytes
   * @return
   */
  private Object deserialize(byte[] bytes) {
    ObjectInputStream oin = null;
    try {
      oin = new ObjectInputStream(new ByteArrayInputStream(bytes));
      return oin.readObject();
    } catch (EOFException eof) {
      return null;
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    } catch (ClassNotFoundException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  /**
   * Delegating Watcher that simply calls the Closures set for that event.
   */
  private class ClientWatcher implements Watcher {
    public void process(WatchedEvent watchedEvent) {
      try {
        Closure callback = null;
        switch (watchedEvent.getType()) {
          case NodeChildrenChanged:
            if (null != onNodeChildrenChanged) {
              callback = onNodeChildrenChanged;
            } else {
              callback = onEvent;
            }
            break;
          case NodeCreated:
            if (null != onNodeCreated) {
              callback = onNodeCreated;
            } else {
              callback = onEvent;
            }
            break;
          case NodeDeleted:
            if (null != onNodeDeleted) {
              callback = onNodeDeleted;
            } else {
              callback = onEvent;
            }
            break;
          case NodeDataChanged:
            if (null != onDataChanged) {
              callback = onDataChanged;
            } else {
              callback = onEvent;
            }
            break;
          case None:
            if (null != onEvent) {
              callback = onEvent;
            }
        }
        if (null != callback) {
          callback.call(watchedEvent);
        } else {
          log.warn("No callbacks defined to accept event: " + watchedEvent);
        }
      } catch (Throwable t) {
        log.debug(t.getMessage());
      }
    }
  }

  /**
   * Small wrapper for dealing with ZooKeeper nodes as objects.
   */
  public class Node extends GroovyObjectSupport {

    private String path;
    private CreateMode type;
    private Stat stat;
    private boolean setWatch = true;

    public Node(String path, CreateMode type) throws InterruptedException, KeeperException {
      this.path = path;
      this.type = type;
      refresh();
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public CreateMode getType() {
      return type;
    }

    public void setType(CreateMode type) {
      this.type = type;
    }

    public Stat getStat() {
      return stat;
    }

    public void setStat(Stat stat) {
      this.stat = stat;
    }

    public boolean isSetWatch() {
      return setWatch;
    }

    public void setSetWatch(boolean setWatch) {
      this.setWatch = setWatch;
    }

    /**
     * Allows you to do <pre><code>node.data = "some data: ${var}"</code></pre> in Groovy code.
     *
     * @param data
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void setData(Object data) throws InterruptedException, KeeperException {
      zookeeper.setData(path, serialize(data), stat.getVersion());
    }

    public Object getData() throws InterruptedException, KeeperException {
      return deserialize(zookeeper.getData(path, setWatch, stat));
    }

    public List<String> getChildren() throws InterruptedException, KeeperException {
      return zookeeper.getChildren(path, false);
    }

    public List<String> getChildren(boolean setWatch) throws InterruptedException, KeeperException {
      return zookeeper.getChildren(path, setWatch);
    }

    /**
     * Re-read the Stat from ZooKeeper.
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void refresh() throws InterruptedException, KeeperException {
      stat = zookeeper.exists(path, setWatch);
    }
  }
}
