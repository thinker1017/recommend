import java.net.InetAddress  
package homed {
object Options {
  private val usage = """
    Usage: analyze [--queryYear yyyy] [--queryMonth mm] [--Columns] [default curMonth]
  """
  def getOptions(map : Map[String, String], list: List[String]) : Map[String, String] = {
    list match {
      case Nil =>  map
      case "--queryMonth" :: value :: tail => getOptions(map + ("queryMonth" -> value), tail)
      case "--queryYear" :: value :: tail => getOptions(map + ("queryYear" -> value), tail)
      case "--columns"  :: tail => getOptions(map + ("columns" -> "yes"), tail)
      case "--stage" :: value :: tail => getOptions(map + ("stage" -> value), tail)
      case option :: tail =>  println("Unknown option "+option); map
    }
  }

  def tool(args: Array[String]): Map[String, String] = {
    if (args.length == 0) println(usage)

    getOptions(Map(), args.toList)
  }

  def getServerId(): String = {
    val hostNamePattern = """slave(\d+).*""".r
    val host = InetAddress.getLocalHost().toString
    try {
      val hostNamePattern(strHostName) = host
      val hostName = strHostName.toInt - 1
      "316%02d".format(hostName)
    } catch {
      case e: Exception => 
        //MyLogger.debug("Unknow host " + host)
        "unknow"
    }
  }
}
}
