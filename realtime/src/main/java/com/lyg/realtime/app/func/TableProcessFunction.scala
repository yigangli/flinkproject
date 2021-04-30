package com.lyg.realtime.app.func

import com.alibaba.fastjson.JSONObject
import com.lyg.realtime.bean.TableProcess
import com.lyg.realtime.common.Config
import com.lyg.realtime.utils.MySQLUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import java.util.{Map, Timer, TimerTask}
import scala.collection.mutable
/**
 * @author: yigang
 * @date: 2021/3/31
 * @desc: 配置表处理函数
 */
class TableProcessFunction(outputTag: OutputTag[JSONObject]) extends ProcessFunction[JSONObject,JSONObject]{

  val log = LoggerFactory.getLogger(classOf[TableProcessFunction])
  val tableProcessMap = mutable.Map[String,TableProcess]()
  val existsTable = mutable.Set[String]()

  var conn:Connection =null

  //函数被调用时执行的方法
  override def open(parameters: Configuration) = {
    println("open方法运行")
    //获取phoenix连接
    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      conn = DriverManager.getConnection(Config.phoenix_server)
    }catch {
      case e:Exception =>e.printStackTrace()
    }
    //初始化配置表信息
    refershMap()
    //开启定时任务，每隔一定时间更新配置表
    //从定时任务被触发起，过过delay毫秒后，每隔period执行一次
    val timer = new Timer()
    timer.schedule(new TimerTask {
      override def run(): Unit = {

        refershMap()
      }
    },5000,5000)


  }


  //每过来一条数据方法执行一次，主要任务是根据配置表中的配置表信息map进行分流处理
  override def processElement(jsonObj: JSONObject, ctx: ProcessFunction[JSONObject, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
    println("收到数据")
    val tbDatabase = jsonObj.getString("database")
    val tableName = jsonObj.getString("table")
    var operateType = jsonObj.getString("type")
    val tableData = jsonObj.getJSONObject("data")
    if("bootstarp-insert".equals(operateType)){
      operateType="insert";
      jsonObj.put("type",operateType)
    }
    if(tableProcessMap!=null&&tableProcessMap.size>0){
      val process: TableProcess = tableProcessMap.getOrElse(tableName+":"+operateType, null)
      if(process!=null){
        //如果在配置表中找到了这条数据对应的配置信息，则在这条数据中添加需要发往的目标表名标记
        jsonObj.put("sink_table",process.getSinkTable)
        //如果指定了sinkColumn，则对需要保留的字段进行过滤
        val sinkColumns = process.getSinkColumns
        if(sinkColumns!=null&&sinkColumns.length>0){
          filterColumn(jsonObj.getJSONObject("data"),sinkColumns)
        }

        //如果这条信息属于hbase维度表则分流到hbase流，否则归入主流
        if(process.getSinkType.equals(TableProcess.SINK_TYPE_HBASE)){
          println("发入hbase流"+jsonObj)
          ctx.output(outputTag,jsonObj)
        }else if(process.getSinkType.equals(TableProcess.SINK_TYPE_KAFKA)){
          println("发入kafka流"+jsonObj)
          collector.collect(jsonObj)
        }
      }else{
        println("No this Key:" + tableName+":"+operateType + " in MySQL")
      }
    }
  }

  def filterColumn(jsonObj: JSONObject, sinkColumns: String) = {
    //sinkColumns 表示需要保留的列
    val fieldsArr = sinkColumns.split(",")
    val entrySet: util.Set[Map.Entry[String, AnyRef]] = jsonObj.entrySet()
    val it = entrySet.iterator()
    while (it.hasNext) {
      val entry = it.next
      if (!fieldsArr.contains(entry.getKey)) it.remove
    }
  }
  def refershMap(): Unit ={
    //从Mysql中查询配置信息
    println("周期读取mysql数据库table_process配置表")
    val processes: List[TableProcess] = MySQLUtil.queryList("select * from table_process", classOf[TableProcess], true)
    for(tableProcess <- processes){
      val key: String = tableProcess.getSourceTable + ":" + tableProcess.getOperateType
      val value :TableProcess = tableProcess
      val sinkType :String = tableProcess.getSinkType
      val sinkTable : String = tableProcess.getSinkTable
      val sinkColumns :String = tableProcess.getSinkColumns
      val sinkPk :String = tableProcess.getSinkPk
      val sinkExtend :String = tableProcess.getSinkExtend
      tableProcessMap.put(key, value)
      if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType)&&"insert".equals(tableProcess.getOperateType)){
        val notExists = existsTable.add(sinkTable)
        if(notExists){
          //如果当前程序缓存中没有创建过该表，检查phonix中是否存在该表，如存在则不创建，如不存在则需要创建
          checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend)
        }
      }
    }

    if(tableProcessMap==null&&tableProcessMap.size==0){
      throw new RuntimeException("没有从数据库中读到配置表数据")
    }

  }

  def checkTable(sinkTable: String, sinkColumns: String, sinkPk: String, sinkExtend: String) = {
    var pk = sinkPk
    var ext = sinkExtend
    if (sinkPk == null) {
      pk = "id"
    }
    if (sinkExtend == null) {
      ext = ""
    }
    val createSql = new StringBuilder("create table if not exists " + Config.hbase_Schema + "." + sinkTable + "(")
    val fieldsArr = sinkColumns.split(",")
    for (i <- 0 until fieldsArr.length) {
      //判断是否为主键字段
      if (pk.equals(fieldsArr(i))) {
        createSql.append(fieldsArr(i)).append(" varchar primary key")
      } else {
        createSql.append("info.").append(fieldsArr(i)).append(" varchar")
      }
      if (i < fieldsArr.length - 1) {
        createSql.append(",")
      }
    }
    createSql.append(")").append(ext)

    println("创建Phoneix表的语句" + createSql)
    var pstmt:PreparedStatement = null
    try {
      pstmt = conn.prepareStatement(createSql.toString())
      pstmt.execute()
      println("创建成功")
    }catch {
      case e:Exception =>e.printStackTrace()
    }finally {
      if(pstmt!=null){
        try{
          pstmt.close()
        }catch {
          case e:Exception=>e.printStackTrace()
            throw new RuntimeException("Phoneix 建表失败")
        }
      }
    }



  }




}
