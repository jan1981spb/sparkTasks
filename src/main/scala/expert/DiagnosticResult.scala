package expert

import scalafx.beans.property.{DoubleProperty, StringProperty}

class DiagnosticResult(state_ : String, maxP_ : String, minP_ : String, aprioriP_ : String) {
  val state = new StringProperty(this, "state", state_)
  val maxP = new StringProperty(this, "maxP", maxP_)
  val minP = new StringProperty(this, "minP", minP_)
  val aprioriP = new StringProperty(this, "aprioriP", aprioriP_)

}
