package expert

import scalafx.beans.property.{IntegerProperty, ObjectProperty, StringProperty}

class Metrics(snapshotTime_ : String, nodeId_ : String, cpuPerCluster_ : String, responseDelayCluster_ : String, responseDelayNode_ : String, statusId_ : String) {
  val snapshotTime     = new StringProperty(this, "snapshotTime", snapshotTime_)
  val nodeId      = new StringProperty(this, "nodeId", nodeId_)
  val cpuPerCluster      = new StringProperty(this, "cpuPerCluster", cpuPerCluster_)
  val responseDelayCluster = new StringProperty(this, "responseDelayCluster", responseDelayCluster_)
  val responseDelayNode = new StringProperty(this, "responseDelayNode", responseDelayNode_)
  val statusId = new StringProperty(this, "statusId", statusId_)
}
