import grails.util.GrailsConfig
import com.jbrisbin.vpc.zk.GroovyZooKeeperHelper

class ZooKeeperGrailsPlugin {
  // the plugin version
  def version = "0.1"
  // the version or versions of Grails the plugin is designed for
  def grailsVersion = "1.3.5 > *"
  // the other plugins this plugin depends on
  def dependsOn = [:]
  // resources that are excluded from plugin packaging
  def pluginExcludes = [
      "grails-app/views/error.gsp"
  ]

  // TODO Fill in these fields
  def author = "J. Brisbin"
  def authorEmail = "jon@jbrisbin.com"
  def title = "Grails ZooKeeper Helper"
  def description = '''\\
Helper plugin to make working with ZooKeeper more Groovyish.
'''

  // URL to the plugin's documentation
  def documentation = "http://github.com/jbrisbin/grails-zookeeper-plugin"

  def doWithWebDescriptor = { xml ->
    // TODO Implement additions to web.xml (optional), this event occurs before
  }

  def doWithSpring = {
    zooKeeper(GroovyZooKeeperHelper, application.config.zooKeeper.url, application.config.zooKeeper.timeout)
  }

  def doWithDynamicMethods = { ctx ->
    // TODO Implement registering dynamic methods to classes (optional)
  }

  def doWithApplicationContext = { applicationContext ->
    // TODO Implement post initialization spring config (optional)
  }

  def onChange = { event ->
    // TODO Implement code that is executed when any artefact that this plugin is
    // watching is modified and reloaded. The event contains: event.source,
    // event.application, event.manager, event.ctx, and event.plugin.
  }

  def onConfigChange = { event ->
    // TODO Implement code that is executed when the project configuration changes.
    // The event is the same as for 'onChange'.
  }
}
