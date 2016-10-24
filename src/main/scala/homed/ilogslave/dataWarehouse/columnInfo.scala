package homed.ilogslave.dataWarehouse
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD
import homed.config.ReConfig._

import java.text.SimpleDateFormat
import java.util.Calendar
import homed.Options
import java.lang.Math
import homed.Jdbc._
import homed.tools._

case class t_column_info(
    f_column_status:Int,
    f_column_id:Long,
    f_column_name:String,
    f_is_hide:Int,
    f_parent_id:Long,
    f_column_index:Int,
    f_description:String,
    f_style:String,
    f_link_info:String
)

case class t_column_program(
    f_column_id:Long,
    f_program_id:Long,
    f_next_program:Long,
    f_is_hide:Int,
    f_is_unsort:Int
)

case class t_duplicate_program(
    f_program_id:Long,
    f_duplicate_id:Long
)

case class t_duplicate_series(
    f_duplicate_id:Long,
    f_program_name:String,
    f_main_program_id:Long
)

//栏目信息
object columnInfo {
  
      //栏目节目树
      private var columnProgram = Array[t_column_program]()
      //series id转换成了duplicate id
      private var duplicateProgram = Array[t_duplicate_program]()
      private var duplicateSeries = Array[t_duplicate_series]()
      
      def Init(sc:SparkContext)={
           val sqlContext = new org.apache.spark.sql.SQLContext(sc)
           val jdbcModel = new columnJdbc("t_column_program",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
           columnProgram = jdbcModel.getColumnProgram(sc, sqlContext)
           jdbcModel.updateInfo("t_duplicate_program",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
           duplicateProgram = jdbcModel.getduplicateProgram(sc, sqlContext)
           jdbcModel.updateInfo("t_duplicate_series",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
           duplicateSeries = jdbcModel.getduplicateSeries(sc, sqlContext) 
      }
      
      def getColumnProgram()={
          columnProgram
      }
      
      def getDuplicateProgram()={
          duplicateProgram
      }
      
      def getDuplicateSeries()={
          duplicateSeries
      }
}