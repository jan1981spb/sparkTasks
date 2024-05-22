package expert

import scalafx.beans.property.StringProperty

class Diagnosis(message_ : String) {
  val message     = new StringProperty(this, "message", message_)
}
