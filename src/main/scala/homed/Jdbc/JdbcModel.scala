package homed.Jdbc
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD
import homed.config.ReConfig._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import homed.Options
import java.lang.Math
import homed.ilogslave.dataWarehouse._
import homed.ilogslave.dataWarehouse.userBehavior._
import homed.tools._

case class videoDes(
      showName:String,
      labelOrTags:String,
      actors:String,
      directors:String,
      description:String
)

case class channelInfo(
      channel_id:Long,
      chinese_name:String
)
class videoDesJdbc extends Serializable{
  private[this] var cond = Array[String]()
  private[this] var table=""
  private[this] var url=""
  private[this] var user=""
  private[this] var pass=""
  private[this] var prop = new java.util.Properties
  
  def this(table:String,url:String,user:String,pass:String){
    this()
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    Class.forName("com.mysql.jdbc.Driver")
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
  }
  def setCondition(cond:Array[String])={
    this.cond = cond
    this
  }
  def updateInfo(table:String,url:String,user:String,pass:String){
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
    
  }
  def getTrainInfoFromd_asset(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val rdd = sqljdbc.map(a=>{
          videoDes(a.getAs[String]("series_name"),a.getAs[String]("labels")
          ,a.getAs[String]("actors"),a.getAs[String]("director")
          ,a.getAs[String]("description")) 
    })
    rdd
  }
  
  def getTrainInfoFromd_program(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val rdd = sqljdbc.map(a=>{
          videoDes(a.getAs[String]("name"),a.getAs[String]("labels")
          ,a.getAs[String]("customer"),""
          ,a.getAs[String]("description")) 
    })
    rdd
  }
  
  def getTrainInfoFromd_bundle(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val rdd = sqljdbc.map(a=>{
          videoDes(a.getAs[String]("folderName"),a.getAs[String]("type")
          ,a.getAs[String]("actorsDisplay"),a.getAs[String]("director")
          ,a.getAs[String]("summarMedium")) 
    })
    rdd
  }
  def getLiveShowInfo(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table")
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val liveShow_infos = sqljdbc.map(a=>{
          videoDes(a.getAs[String]("f_showName"),a.getAs[String]("f_tab"),a.getAs[String]("f_actors"),
              a.getAs[String]("f_director"),RTool.replaceUnLikeChar(new String(a.getAs[Array[Byte]]("f_showInfo"))))
    })
    
    liveShow_infos
  }
  
}


object videoJdbc extends Serializable{
  
}
class videoJdbc extends Serializable{
  private[this] var cond = Array[String]()
  private[this] var table=""
  private[this] var url=""
  private[this] var user=""
  private[this] var pass=""
  private[this] var prop = new java.util.Properties
  
  def this(table:String,url:String,user:String,pass:String){
    this()
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    Class.forName("com.mysql.jdbc.Driver")
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
  }
  def setCondition(cond:Array[String])={
    this.cond = cond
    this
  }
  def updateInfo(table:String,url:String,user:String,pass:String){
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
    
  }
  def getVideoSeries(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val rdd = sqljdbc.map(a=>{
          video_serie(a.getAs[Long]("series_id"),a.getAs[String]("series_name"),a.getAs[Long]("service_id"),
          a.getAs[Long]("type"),new String(a.getAs[Array[Byte]]("labels")),a.getAs[Int]("series_num"),a.getAs[String]("actors"),a.getAs[String]("director")
          ,a.getAs[String]("country"),a.getAs[String]("language"),RTool.replaceUnLikeChar(new String(a.getAs[Array[Byte]]("description")))
          ,a.getAs[Int]("status"),a.getAs[String]("series_year"),a.getAs[java.sql.Timestamp]("f_edit_time")
          ,a.getAs[String]("f_tab"),a.getAs[Int]("f_score"),a.getAs[String]("f_tag")) 
    })
    rdd.collect()
  }
  
  def getVideoInfos(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val rdd = sqljdbc.map(a=>{
          video_info(a.getAs[java.math.BigDecimal]("video_id").longValue(),a.getAs[String]("video_name"),"",
           RTool.replaceUnLikeChar(new String(a.getAs[Array[Byte]]("descriptions"))),
           a.getAs[java.sql.Timestamp]("register_date"),a.getAs[String]("direct_list"),a.getAs[String]("actor_list")
          ,a.getAs[java.sql.Timestamp]("video_time"),a.getAs[String]("language"),a.getAs[String]("country")
          ,a.getAs[String]("f_tab"),a.getAs[String]("f_tag"),a.getAs[Long]("f_series_id")) 
    })
    rdd.collect()
  }
}

object columnJdbc extends Serializable{

}
class columnJdbc extends Serializable{
  private[this] var cond = Array[String]()
  private[this] var table=""
  private[this] var url=""
  private[this] var user=""
  private[this] var pass=""
  private[this] var prop = new java.util.Properties
  
  def this(table:String,url:String,user:String,pass:String){
    this()
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    Class.forName("com.mysql.jdbc.Driver")
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
  }
  def updateInfo(table:String,url:String,user:String,pass:String){
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
    
  }
  def setCondition(cond:Array[String])={
    this.cond = cond
    this
  }
  def getColumnProgram(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val rdd = sqljdbc.map(a=>{
          t_column_program(a.getAs[Long]("f_column_id"),a.getAs[Long]("f_program_id"),a.getAs[Long]("f_next_program"),
          a.getAs[Int]("f_is_hide"),a.getAs[Int]("f_is_unsort")) 
    })
    rdd.collect()
  }
  
  def getduplicateProgram(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val rdd = sqljdbc.map(a=>{
          t_duplicate_program(a.getAs[Long]("f_program_id"),a.getAs[Long]("f_duplicate_id")) 
    })
    rdd.collect()
  }
  
  def getduplicateSeries(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val rdd = sqljdbc.map(a=>{
          t_duplicate_series(a.getAs[Long]("f_duplicate_id"),a.getAs[String]("f_program_name"),a.getAs[Long]("f_main_program_id")) 
    })
    rdd.collect()
  }
  
}



object userJdbc extends Serializable{
  

}
class userJdbc extends Serializable{
  private[this] var cond = Array[String]()
  private[this] var table=""
  private[this] var url=""
  private[this] var user=""
  private[this] var pass=""
  private[this] var prop = new java.util.Properties
  
  def this(table:String,url:String,user:String,pass:String){
    this()
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    Class.forName("com.mysql.jdbc.Driver")
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
  }
  def setCondition(cond:Array[String])={
    this.cond = cond
    this
  }
  def updateInfo(table:String,url:String,user:String,pass:String){
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
    
  }
 
   
  def getUserInfo(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,${cond}")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val userinfo = sqljdbc.map(a=>{
          account_info(a.getAs[Long]("DA"),a.getAs[String]("account_name"),a.getAs[String]("sex").toInt,a.getAs[java.sql.Date]("birthday"),a.getAs[java.sql.Timestamp]("create_time"),
          a.getAs[java.sql.Timestamp]("last_login_time"),a.getAs[java.sql.Timestamp]("last_logout_time"),a.getAs[Long]("count_online_time"),a.getAs[Int]("age")  ) 
    })
    userinfo.collect()
  }
}

object showJdbc extends Serializable{
  private val SaveNameMap = Map("event_series"->"EventSeries","homed_eit_schedule_history"->"eitschdules") 
}

class showJdbc extends Serializable {
  private[this] var cond = Array[String]()
  private[this] val fs = FileSystem.get(new Configuration())
  private[this] var saveingFilename = ""
  private[this] var savedFilename = ""
  private[this] var table=""
  private[this] var url=""
  private[this] var user=""
  private[this] var pass=""
  private[this] var dateboundary = 0L
  private[this] var prop = new java.util.Properties

  def this(table:String,url:String,user:String,pass:String){
    this()
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    Class.forName("com.mysql.jdbc.Driver")
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
    updateFileInfo
  }
  def updateInfo(table:String,url:String,user:String,pass:String){
    this.table = table
    this.url = url
    this.user = user
    this.pass = pass
    prop.setProperty("user", user)
    prop.setProperty("password", pass)
    prop.setProperty("zeroDateTimeBehavior", "convertToNull")
    updateFileInfo
  }
  private def updateFileInfo(){
    val calendar = Calendar.getInstance()
    val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dateStr = if(SAVINGFILEDATA == "00-00-00") ymdFormat.format(calendar.getTime) else SAVINGFILEDATA
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    val yesterdayStr = if(BACKUPFILEDATA == "00-00-00")  ymdFormat.format(calendar.getTime) else BACKUPFILEDATA   
    MyLogger.debug(s"now:${dateStr},yesterday:${yesterdayStr}")
    dateboundary = ymdFormat.parse(yesterdayStr).getTime
    
    saveingFilename = ilogslaveRddSavePrefix+dateStr+"/"+showJdbc.SaveNameMap.getOrElse(table,"")+".data"+SAVINGDATAINDEX
    savedFilename = ilogslaveRddSavePrefix+yesterdayStr+"/"+showJdbc.SaveNameMap.getOrElse(table,"")+".data"+SAVEDDATAINDEX
    MyLogger.debug(s"savedFilename:${savedFilename},saveingFilename:${saveingFilename},dateboundary=${dateboundary}")

  }
  def setCondition(cond:Array[String])={
    this.cond = cond
    this
  }
  def getEventSeries(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table")
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    var result = ListBuffer[RDD[event_serie]]()
    val sqlrdd = sqljdbc.map(a=>{
             event_serie(a.getAs[Long]("series_id"),a.getAs[String]("series_name"),a.getAs[Long]("service_id"),a.getAs[String]("actors"),a.getAs[String]("director"), RTool.replaceUnLikeChar(new String(a.getAs[Array[Byte]]("description"))),a.getAs[String]("f_tab"),a.getAs[java.sql.Timestamp]("f_edit_time"))
    })
    sqlrdd      
    /*try{
       if(fs.exists(new Path(savedFilename))){
          MyLogger.debug(savedFilename)
          val saveRdd = sc.objectFile[event_serie](savedFilename)
          val t = dateboundary
          //saveRdd.collect.foreach(x=>MyLogger.debug(x.toString()))
          val sqlrdd = sqljdbc.map(a=>{
             event_serie(a.getAs[Long]("series_id"),a.getAs[String]("series_name"),a.getAs[Long]("service_id"),a.getAs[String]("actors"),a.getAs[String]("director"), RTool.replaceUnLikeChar(new String(a.getAs[Array[Byte]]("description"))),a.getAs[String]("f_tab"),a.getAs[java.sql.Timestamp]("f_edit_time"))
          }).filter(x=>{x.f_edit_time.getTime > t})
          MyLogger.debug(s"has backup,$savedFilename},size=${sqlrdd.count()},${saveRdd.count()}")
          val union_rdd = sc.union(sqlrdd,saveRdd).distinct()
          
          //union_rdd.foreach(x=>MyLogger.debug(x.toString()))
          try{
             result += union_rdd
             union_rdd.saveAsObjectFile(saveingFilename)
          }catch{
             case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
             MyLogger.debug(s"saveAsObjectFile saveingFilename err")
          }
       }else{
          MyLogger.debug(s"no backup,$savedFilename")
          val sqlrdd = sqljdbc.map(a=>{
              event_serie(a.getAs[Long]("series_id"),a.getAs[String]("series_name"),a.getAs[Long]("service_id"),a.getAs[String]("actors"),a.getAs[String]("director"), RTool.replaceUnLikeChar(new String(a.getAs[Array[Byte]]("description"))),a.getAs[String]("f_tab"),a.getAs[java.sql.Timestamp]("f_edit_time")) 
          })
          try{
             result += sqlrdd
             sqlrdd.saveAsObjectFile(saveingFilename)
          }catch{
             case e:Exception => {MyLogger.debug("err"+e)}
             MyLogger.debug(s"saveAsObjectFile $saveingFilename err")
          }
          MyLogger.debug(s"first save: $saveingFilename,${result.size}")
          
       }
    }catch{
       case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
    }finally {
       result
    }
    sc.union(result)*/
  }
  
  
  /*def getLiveShowInfo(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table")
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val liveShow_infos = sqljdbc.map(a=>{
          liveShow_info(a.getAs[Long]("f_show_id"),a.getAs[String]("f_showName"),a.getAs[String]("f_channelName"),RTool.replaceUnLikeChar(new String(a.getAs[Array[Byte]]("f_showInfo"))),a.getAs[Int]("f_showId"),
              a.getAs[String]("f_tab"),a.getAs[String]("f_actors"),a.getAs[String]("f_director"),a.getAs[String]("f_area"),a.getAs[String]("f_releaseYear"),a.getAs[java.sql.Timestamp]("f_date")) 
    })
    
    if(dataTest == 1)liveShow_infos.collect().take(1000) else liveShow_infos.collect()
  }*/
  
  def getShowRelateInfo(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table")
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    
    val ShowRelate = sqljdbc.map(a=>{
          t_show_relate(a.getAs[Long]("f_program_id"),a.getAs[Long]("f_series_id"),a.getAs[java.sql.Timestamp]("f_date"),a.getAs[Int]("f_relate_level")) 
    })
    ShowRelate.collect()
    
  }
  def getChannelInfo(sc:SparkContext,sqlContext:SQLContext)={
      MyLogger.debug(s"$url,$table,$cond")
      cond.foreach(x=>MyLogger.debug(x.toString()))
      val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
      val channels = sqljdbc.map(a=>{
            channelInfo(a.getAs[Long]("channel_id"),a.getAs[String]("chinese_name")) 
      })
      channels
  }
  def getEitSchedule(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,$cond")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    val eits = sqljdbc.map(a=>{
          homed_eit_schedule(a.getAs[Long]("homed_service_id"),a.getAs[Int]("event_id"),a.getAs[String]("event_name"),a.getAs[java.sql.Timestamp]("start_time"),a.getAs[java.sql.Timestamp]("duration")) 
    })
    eits
  }
  
  def getEitScheduleHistory(sc:SparkContext,sqlContext:SQLContext)={
    MyLogger.debug(s"$url,$table,$cond")
    cond.foreach(x=>MyLogger.debug(x.toString()))
    val sqljdbc = if(cond.size > 0 )sqlContext.read.jdbc(url,table,cond, prop) else sqlContext.read.jdbc(url,table, prop)
    var result = ListBuffer[RDD[homed_eit_schedule_history]]()
    val sqlrdd = sqljdbc.map(a=>{
              homed_eit_schedule_history(a.getAs[Long]("homed_service_id"),a.getAs[Int]("event_id"),a.getAs[String]("event_name"),a.getAs[java.sql.Timestamp]("start_time"),a.getAs[java.sql.Timestamp]("duration"),a.getAs[java.sql.Timestamp]("f_edit_time"),a.getAs[Long]("f_series_id")) 
          })
    sqlrdd
    /*try{
       if(fs.exists(new Path(savedFilename))){
          MyLogger.debug(savedFilename)
          val saveRdd = sc.objectFile[homed_eit_schedule_history](savedFilename)
          //saveRdd.collect.foreach(x=>MyLogger.debug(x.toString()))
          val sqlrdd = sqljdbc.map(a=>{
              homed_eit_schedule_history(a.getAs[Long]("homed_service_id"),a.getAs[Int]("event_id"),a.getAs[String]("event_name"),a.getAs[java.sql.Timestamp]("start_time"),a.getAs[java.sql.Timestamp]("duration"),a.getAs[java.sql.Timestamp]("f_edit_time"),a.getAs[Long]("f_series_id")) 
          })
          MyLogger.debug(s"has backup,$savedFilename},size=${sqlrdd.count()},${saveRdd.count()}")
          val union_rdd = sc.union(sqlrdd,saveRdd).distinct()
          //union_rdd.foreach(x=>MyLogger.debug(x.toString()))
          try{
             result += union_rdd
             union_rdd.saveAsObjectFile(saveingFilename)
          }catch{
             case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
             MyLogger.debug(s"saveAsObjectFile saveingFilename err")
          }  
       }else{
          MyLogger.debug(s"no backup,$savedFilename")
          val t = dateboundary
          val sqlrdd = sqljdbc.map(a=>{
              homed_eit_schedule_history(a.getAs[Long]("homed_service_id"),a.getAs[Int]("event_id"),a.getAs[String]("event_name"),a.getAs[java.sql.Timestamp]("start_time"),a.getAs[java.sql.Timestamp]("duration"),a.getAs[java.sql.Timestamp]("f_edit_time"),a.getAs[Long]("f_series_id")) 
          })
          try{
             result += sqlrdd
             sqlrdd.saveAsObjectFile(saveingFilename)
          }catch{
             case e:Exception => {MyLogger.debug("err"+e)}
             MyLogger.debug(s"saveAsObjectFile $saveingFilename err")
          }
          MyLogger.debug(s"first save: $saveingFilename")
          
       }
    }catch{
       case e:Exception => {e.printStackTrace();MyLogger.debug("err"+e)}
    }finally {
       result
    }
    sc.union(result)*/
    
  }
  
}