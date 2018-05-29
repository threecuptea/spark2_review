package org.freemind.spark.sql

import java.util.regex.{Matcher, Pattern}


case class DanubeStates (
                          roviId: Long,
                          resource: String,
                          state: String,
                          pubId: Long,
                          jtNo: Integer,
                          jtYes: Integer
                        )

case class DanubeResolverRaw (
                               resource: String,
                               roviId: Long,
                               pubId: Long,
                               old_pubId: String,
                               state: String,
                               dirty_size: Integer
                             )


class DanubeLogParser(a: Option[Array[String]] = None) extends Serializable {

  val nonJtLogRegEx = "\\[listener\\-\\d{1}\\] - (PUBLISH|NOPUBLISH|UNPUBLISH) (\\w+) (\\d+) \\((\\d+)\\)"
  val jtLogRegEx    = "\\[listener\\-\\d{1}\\] - (PUBLISH|NOPUBLISH|UNPUBLISH) (\\w+)\\-(\\d+) \\((\\d+)\\)"

  val nonjtPattern = Pattern.compile(nonJtLogRegEx)
  val jtPattern = Pattern.compile(jtLogRegEx)

  def resourcesConcat = a.mkString("|")

  def nonJtDetailPattern(): Pattern = {
    val regEx = s"RESOLVE (${resourcesConcat}) (\\d+) \\((\\d+) replacing (\\w+)\\) , (\\d+) dirty"
    Pattern.compile(regEx)
  }

  def jtDetailPattern(): Pattern = {
    val regEx = s"RESOLVE (${resourcesConcat})\\-(\\d+) \\((\\d+) replacing (\\w+)\\) , (\\d+) dirty"
    Pattern.compile(regEx)
  }

  def parseNonJtLog2(s: String): Option[DanubeStates] = {
    val m: Matcher = nonjtPattern.matcher(s)
    if (m.find) {
      Some(DanubeStates(
        roviId = m.group(3).toLong,
        resource = m.group(2),
        state = m.group(1),
        pubId = m.group(4).toLong,
        jtNo = 1,
        jtYes = 0
      ))
    }
    else {
      None
    }
  }

  def parseJtLog2(s: String): Option[DanubeStates] = {
    val m: Matcher = nonjtPattern.matcher(s)
    if (m.find) {
      Some(DanubeStates(
        roviId = m.group(3).toLong,
        resource = m.group(2),
        state = m.group(1),
        pubId = m.group(4).toLong,
        jtNo = 0,
        jtYes = 1
      ))
    }
    else {
      None
    }
  }

  def parseResolverRaw(s: String, jt: Boolean = false): Option[DanubeResolverRaw] = {
    val m: Matcher = if (jt) jtDetailPattern.matcher(s) else nonJtDetailPattern.matcher(s)
    if (m.find) {
      Some(DanubeResolverRaw(
        resource = m.group(1),
        roviId = m.group(2).toLong,
        pubId = m.group(3).toLong,
        old_pubId = m.group(4),
        state = m.group(4) match {
          case "empty" => "new"
          case _ =>   "update"
        },
        dirty_size = m.group(5).toInt
      ))
    }
    else {
      None
    }

  }


}
