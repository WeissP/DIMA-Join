package de.rptu.cs.dbis.utils

extension [T](v: T | Null) {
  def toOption: Option[T] = {
    if (v == null) { None }
    else { Some(v.nn) }
  }
}

def curTimestamp: String = {
  val sdf = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").nn
  sdf.format(new java.util.Date()).nn
}
