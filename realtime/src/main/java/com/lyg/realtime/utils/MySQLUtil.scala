package com.lyg.realtime.utils

import com.google.common.base.CaseFormat
import com.lyg.realtime.bean.TableProcess
import org.apache.commons.beanutils.BeanUtils
import sun.plugin.com.JavaClass

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

/**
 * @author: yigang
 * @date: 2021/3/31
 * @desc: 从MYSQL数据库中查询数据的工具类
 * 完成ORM：对象关系映射
 * O:Object     JAVA对象
 * R:Relation   关系型数据库
 * M:Mapping    JAVA对象和关系型数据库的映射
 */
object MySQLUtil {

  /**
   *
   * @param sql  执行的查询语句
   * @param clz  返回的数据类型
   * @param underScoreToCamel  是否将下划线转换为驼峰命名法
   * @tparam T
   * @return 返回结果集
   */
  def queryList[T](sql:String,clz:Class[T],underScoreToCamel:Boolean):List[T]={
    var conn:Connection = null
    var stmt:PreparedStatement = null
    var rs:ResultSet = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(
        "jdbc:mysql://cdh-node3:3306/data_sample_realtime?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8",
        "root",
        "ZHI123zhi")
      stmt = conn.prepareStatement(sql)
      rs = stmt.executeQuery(sql)
      val metaData: ResultSetMetaData = rs.getMetaData
      var resultList:List[T] = List()
      while(rs.next()){
        val obj:T= clz.newInstance()
        for(i <- 1 to metaData.getColumnCount){
          val columnName = metaData.getColumnName(i)
          val columnValue = rs.getString(columnName)
          if(underScoreToCamel){
            //如果指定将下划线转换为驼峰命名法的值为true，需要将表中的列转换为驼峰命名法形式
            val colPropertyName: String = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName)
            BeanUtils.setProperty(obj,colPropertyName,columnValue)
          }
        }
        resultList = resultList:+obj
      }
      resultList
    }catch {
      case e:Exception =>e.printStackTrace()
        throw new RuntimeException("从mysql查询数据失败")
    }finally {
      if(rs!=null){
        try{
          rs.close()
        }catch {
          case e: Exception=>e.printStackTrace()
        }
      }
      if(stmt!=null){
        try{
          stmt.close()
        }catch {
          case e: Exception=>e.printStackTrace()
        }
      }
      if(conn!=null){
        try{
          conn.close()
        }catch {
          case e: Exception=>e.printStackTrace()
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val list = queryList("select * from table_process",classOf[TableProcess],true)
    for(i<-list){
      println(i)
    }
  }
}
