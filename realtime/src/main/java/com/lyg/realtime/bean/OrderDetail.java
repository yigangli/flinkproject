package com.lyg.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Author: Felix
 * Date: 2021/2/5
 * Desc: 订单明细实体类
 */
@Data
public class OrderDetail {
    Long id;
    Long order_id ;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getOrder_id() {
        return order_id;
    }

    public void setOrder_id(Long order_id) {
        this.order_id = order_id;
    }

    public Long getSku_id() {
        return sku_id;
    }

    public void setSku_id(Long sku_id) {
        this.sku_id = sku_id;
    }

    public BigDecimal getOrder_price() {
        return order_price;
    }

    public void setOrder_price(BigDecimal order_price) {
        this.order_price = order_price;
    }

    public Long getSku_num() {
        return sku_num;
    }

    public void setSku_num(Long sku_num) {
        this.sku_num = sku_num;
    }

    public String getSku_name() {
        return sku_name;
    }

    public void setSku_name(String sku_name) {
        this.sku_name = sku_name;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public BigDecimal getSplit_total_amount() {
        return split_total_amount;
    }

    public void setSplit_total_amount(BigDecimal split_total_amount) {
        this.split_total_amount = split_total_amount;
    }

    public BigDecimal getSplit_activity_amount() {
        return split_activity_amount;
    }

    public void setSplit_activity_amount(BigDecimal split_activity_amount) {
        this.split_activity_amount = split_activity_amount;
    }

    public BigDecimal getSplit_coupon_amount() {
        return split_coupon_amount;
    }

    public void setSplit_coupon_amount(BigDecimal split_coupon_amount) {
        this.split_coupon_amount = split_coupon_amount;
    }

    public Long getCreate_ts() {
        return create_ts;
    }

    public void setCreate_ts(Long create_ts) {
        this.create_ts = create_ts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderDetail that = (OrderDetail) o;
        return Objects.equals(id, that.id) && Objects.equals(order_id, that.order_id) && Objects.equals(sku_id, that.sku_id) && Objects.equals(order_price, that.order_price) && Objects.equals(sku_num, that.sku_num) && Objects.equals(sku_name, that.sku_name) && Objects.equals(create_time, that.create_time) && Objects.equals(split_total_amount, that.split_total_amount) && Objects.equals(split_activity_amount, that.split_activity_amount) && Objects.equals(split_coupon_amount, that.split_coupon_amount) && Objects.equals(create_ts, that.create_ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, order_id, sku_id, order_price, sku_num, sku_name, create_time, split_total_amount, split_activity_amount, split_coupon_amount, create_ts);
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
                "id=" + id +
                ", order_id=" + order_id +
                ", sku_id=" + sku_id +
                ", order_price=" + order_price +
                ", sku_num=" + sku_num +
                ", sku_name='" + sku_name + '\'' +
                ", create_time='" + create_time + '\'' +
                ", split_total_amount=" + split_total_amount +
                ", split_activity_amount=" + split_activity_amount +
                ", split_coupon_amount=" + split_coupon_amount +
                ", create_ts=" + create_ts +
                '}';
    }
}

