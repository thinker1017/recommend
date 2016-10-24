package homed.Jdbc

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import homed.Options
import homed.config.ReConfig._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD
import java.sql.{ Connection, DriverManager,Date }
import homed.tools._

/*
use homed_ilog;
CREATE tabLE t_itemcf_recommend (
f_program_id int(10) unsigned NOT NULL COMMENT '节目id',
f_index int(10) unsigned NOT NULL COMMENT '推荐节目对应序号',
f_recommend_id int(10) unsigned NOT NULL COMMENT '推荐节目id',
f_play_time  datetime NOT NULL COMMENT '推荐节目对应时间' ,
f_recommend_reason varchar(255) NOT NULL COMMENT '推荐原因',
f_recommend_date     datetime NOT NULL COMMENT '数据更新时间',
PRIMARY KEY(f_program_id, f_index)
)ENGINE=InnoDB default charset=utf8 COMMENT='相似节目推荐信息';

use homed_ilog;
CREATE tabLE t_content_recommend(
f_program_id int(10) unsigned  NOT NULL COMMENT '节目id',
f_tag_name varchar(255)  NOT NULL COMMENT '标签名称',
f_index int(10) unsigned NOT NULL COMMENT '推荐节目对应序号',
f_recommend_id int(10) unsigned  NOT NULL COMMENT '推荐节目id',
f_play_time datetime NOT NULL COMMENT '推荐节目对应时间' ,
f_recommend_reason varchar(255) NOT NULL COMMENT '推荐原因',
f_recommend_date  datetime  NOT NULL COMMENT '数据更新时间',
PRIMARY KEY(f_program_id, f_index)
)ENGINE=InnoDB  default charset=utf8 COMMENT='标签节目推荐信息';
 * 
 * 
 * */
//item 相似推荐
/*case class t_itemcf_recommend(
    f_program_id:Long,
    f_ritem_list:Array[Long],
    f_play_time:Array[Long],
    f_ritem_reason:Array[Long],
    f_recommend_date:java.sql.Timestamp
)
//tab推荐
case class t_content_recommend(
    f_series_id:Long,
    f_tag_name:String,
    f_recommend_series:Array[Long],
    f_date:java.sql.Timestamp   
)*/

case class t_itemcf_recommend(
    f_program_id:Long,
    f_index:Int,
    f_recommend_id:Long,
    f_play_time:java.sql.Timestamp,
    f_recommend_reason:String,
    f_recommend_date:java.sql.Timestamp
)

case class t_content_recommend (
    f_program_id:Long,
    f_tag_name:String,
    f_index:Int,
    f_recommend_id:Long,
    f_play_time:java.sql.Timestamp,
    f_recommend_reason:String,
    f_recommend_date:java.sql.Timestamp
)


//节目与event_series关联表,包含过去节目和未来节目
case class t_show_relate(
    f_program_id:Long,
    f_series_id:Long,
    f_date:java.sql.Timestamp,
    f_relate_level:Int
)




object resultJdbc {
  
     private def writeToByte(n: Long)={
          val arrayByte = new ArrayBuffer[Byte]()
          var m = n
          while ((m & ~0x7F) != 0) {
            arrayByte += ((m & 0x7F) | 0x80).toByte
            m = m >>> 7
          }
          arrayByte += m.toByte
          arrayByte
     }
     
     def saveShowRelate(sc:SparkContext,arr:Array[t_show_relate])={
          /*MyLogger.debug("save ShowRelate to mysql")
          val connection = DriverManager.getConnection(IlogJDBCurl+"?characterEncoding=UTF-8", DtvsUser, DtvsPasswd)
          val statement = connection.createStatement
           //每天清空
          val preStatement = connection.prepareStatement("insert into t_show_relate (f_program_id,f_series_id,f_date,f_relate_level) values (?,?,?,?)")
          statement.executeUpdate("Truncate table t_show_relate")
          
          
          arr.foreach(x=>{
              preStatement.setLong(1, x.f_program_id)
              preStatement.setLong(2, x.f_series_id)
              preStatement.setTimestamp(3, x.f_date)
              preStatement.setInt(4, x.f_relate_level)
              preStatement.executeUpdate()
          })  */
     }
     def deleteHistorydateDate()={
          val calendar = Calendar.getInstance()
          val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
          val today = ymdFormat.format(calendar.getTime)
          calendar.add(Calendar.DAY_OF_YEAR, -1)
          val Yesterday = ymdFormat.format(calendar.getTime)
          today
     }
     def savetabrecommend(sc:SparkContext,arr:Array[t_content_recommend],newdatadate:String)={
          MyLogger.debug("save tabrecommend to mysql")
          val connection = DriverManager.getConnection(IlogJDBCUrl+"?characterEncoding=UTF-8", IlogUser, IlogPasswd)
          val statement = connection.createStatement
         // val oldDate = deleteHistorydateDate
          //每天清空
          statement.executeUpdate("Truncate table t_content_recommend")
          //statement.executeUpdate(s"delete from t_content_recommend where f_recommend_date <= '$oldDate' or f_recommend_date >= '$newdatadate'")
          val preStatement = connection.prepareStatement("insert into t_content_recommend (f_program_id,f_tag_name,f_index,f_recommend_id,f_play_time,f_recommend_reason,f_recommend_date) values (?,?,?,?,?,?,?)")
          arr.foreach(x=>{
              preStatement.setLong(1, x.f_program_id)
              preStatement.setString(2, x.f_tag_name)
              preStatement.setLong(3, x.f_index)
              preStatement.setLong(4, x.f_recommend_id)
              preStatement.setTimestamp(5, x.f_play_time)
              preStatement.setString(6, x.f_recommend_reason)
              preStatement.setTimestamp(7, x.f_recommend_date)
              try{
                  preStatement.executeUpdate()
              }
              catch{
                 case e:Exception => {
                   e.printStackTrace()
                   MyLogger.debug("err"+e)
                   MyLogger.debug(s"save to db err ${x}")
                 }
              }
          })
   
     }
     
     
     
     def saveItemrecommend(sc:SparkContext,arr:Array[t_itemcf_recommend])={
          MyLogger.debug("save sim to mysql")
          val connection = DriverManager.getConnection(IlogJDBCUrl+"?characterEncoding=UTF-8", IlogUser, IlogPasswd)
          val statement = connection.createStatement
           //每天清空
          statement.executeUpdate("Truncate table t_itemcf_recommend")
          //f_rlive_reason todo
          val preStatement = connection.prepareStatement("insert into t_itemcf_recommend (f_program_id,f_index,f_recommend_id,f_play_time,f_recommend_reason,f_recommend_date) values (?,?,?,?,?,?)")
          val date = new java.sql.Date(new java.util.Date().getTime)
          
          arr.foreach(x=>{
              preStatement.setLong(1, x.f_program_id)
              preStatement.setLong(2, x.f_index)
              preStatement.setLong(3, x.f_recommend_id)
              preStatement.setTimestamp(4, x.f_play_time)
              preStatement.setString(5, x.f_recommend_reason)
              preStatement.setTimestamp(6, x.f_recommend_date)
              try{
                  preStatement.executeUpdate()
              }
              catch{
                 case e:Exception => {
                   e.printStackTrace()
                   MyLogger.debug("err"+e)
                   MyLogger.debug(s"save to db err ${x}")
                 }
              }
             
          })
     }
}