package com.lyg.realtime.bean;

import lombok.Data;

import java.util.Objects;

/**
 * Author: Felix
 * Date: 2021/2/1
 * Desc:  配置表对应的实体类
 */
@Data
public class TableProcess {
    //动态分流Sink常量   改为小写和脚本一致
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public String getSinkPk() {
        return sinkPk;
    }

    public void setSinkPk(String sinkPk) {
        this.sinkPk = sinkPk;
    }

    public String getSinkExtend() {
        return sinkExtend;
    }

    public void setSinkExtend(String sinkExtend) {
        this.sinkExtend = sinkExtend;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableProcess that = (TableProcess) o;
        return Objects.equals(sourceTable, that.sourceTable) && Objects.equals(operateType, that.operateType) && Objects.equals(sinkType, that.sinkType) && Objects.equals(sinkTable, that.sinkTable) && Objects.equals(sinkColumns, that.sinkColumns) && Objects.equals(sinkPk, that.sinkPk) && Objects.equals(sinkExtend, that.sinkExtend);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTable, operateType, sinkType, sinkTable, sinkColumns, sinkPk, sinkExtend);
    }

    @Override
    public String toString() {
        return "TableProcess{" +
                "sourceTable='" + sourceTable + '\'' +
                ", operateType='" + operateType + '\'' +
                ", sinkType='" + sinkType + '\'' +
                ", sinkTable='" + sinkTable + '\'' +
                ", sinkColumns='" + sinkColumns + '\'' +
                ", sinkPk='" + sinkPk + '\'' +
                ", sinkExtend='" + sinkExtend + '\'' +
                '}';
    }
}

