## Grails ZooKeeper Plugin

This is a port of the GroovyZooKeeperHelper that is part of my
Virutal Private Cloud Utilities:

(http://github.com/jbrisbin/vpc-utils)[http://github.com/jbrisbin/vpc-utils]

It allows you to interact with ZooKeeper in a more Grails-friendly way:

<pre><code>class MyController {

  // Auto-injected by the plugin
  def zooKeeper

  def action = {
    zooKeeper.onDataChanged = { evt ->
      // We're interested in data changing while we're watching
      println "data changed: ${evt}"
    }
    if (!zk.exists("/my/node/parent")) {
      zk.createPersistentNodeAndParents("/my/node/parent")
    }
    node = zk.createPersistentNode("/my/node/parent/child")
    node.data = 12345
  }

}
</code></pre>