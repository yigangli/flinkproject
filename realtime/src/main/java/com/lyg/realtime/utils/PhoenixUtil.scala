package com.lyg.realtime.utils

import com.lyg.realtime.common.Config
import org.apache.commons.beanutils.BeanUtils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author: yigang
 * @date: 2021/5/14
 * @desc:
 */
object PhoenixUtil {
  var conn:Connection = null
  def main(args: Array[String]): Unit = {

  }
  def queryInit()={
    try{
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      conn = DriverManager.getConnection(Config.phoenix_server)
      conn.setSchema(Config.hbase_Schema)
    }catch {
      case e:Exception=>e.printStackTrace()
    }

  }

  def queryList[T](sql:String,clazz:Class[T]):List[T]={
    if(conn == null){
      queryInit()
    }
    var resultList = List[T]()
    var ps : PreparedStatement= null
    try{
      ps = conn.prepareStatement(sql)
      val rs = ps.executeQuery
      val md = rs.getMetaData
      while(rs.next()){
        val rowData = clazz.newInstance()
        for(i <- 1 to md.getColumnCount){
          BeanUtils.setProperty(rowData,md.getColumnName(i),rs.getObject(i))
        }
        resultList = resultList:+rowData

      }
      rs.close()
      ps.close()
    }catch {
      case e:Exception=>e.printStackTrace()
    }
    resultList
  }
}
