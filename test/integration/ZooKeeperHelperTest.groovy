/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
class ZooKeeperHelperTest extends GroovyTestCase {

  def zooKeeper

  void testLocalZooKeeper() {

    println "Making sure GroovyZooKeeperHelper has been set properly..."
    assert null != zooKeeper

    println "Creating an ephemeral test node..."
    def node = zooKeeper.create("/zkhelpertest", "test data")

    println "Does 'test data' == node.data? ${node.data == "test data"}"
    assert node.data == "test data"
  }
}