## Grails ZooKeeper Plugin

This is a port of the GroovyZooKeeperHelper that is part of my
Virutal Private Cloud Utilities:

[http://github.com/jbrisbin/vpc-utils](http://github.com/jbrisbin/vpc-utils)

It allows you to interact with ZooKeeper in a more Grails-friendly way:

<pre><code>class MyController {

  // Auto-injected by the plugin
  def zooKeeper

  def action = {
    zooKeeper.onDataChanged = { evt ->
      // We're interested in data changing while we're watching
      println "data changed: ${evt}"
    }
    if (!zooKeeper.exists("/my/node/parent")) {
      zooKeeper.createPersistentNodeAndParents("/my/node/parent")
    }
    node = zooKeeper.createPersistentNode("/my/node/parent/child")
    node.data = 12345
  }

}
</code></pre>

### Configuration

To use the ZooKeeper helper with your own ZooKeeper servers, you need
to specify a valid ZooKeeper URL in your Config.groovy:

<pre><code>zooKeeper.url = "localhost:2181"
zooKeeper.timeout = 20000
</code></pre>

### Documentation

[http://jbrisbin.github.com/grails-zookeeper-plugin/](http://jbrisbin.github.com/grails-zookeeper-plugin/)
