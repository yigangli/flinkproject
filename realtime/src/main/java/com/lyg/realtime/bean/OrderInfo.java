package com.lyg.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Author: Felix
 * Date: 2021/2/5
 * Desc: 订单实体类
 */
@Data
public class OrderInfo {

    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;
    Long create_ts;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getProvince_id() {
        return province_id;
    }

    public void setProvince_id(Long province_id) {
        this.province_id = province_id;
    }

    public String getOrder_status() {
        return order_status;
    }

    public void setOrder_status(String order_status) {
        this.order_status = order_status;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public BigDecimal getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(BigDecimal total_amount) {
        this.total_amount = total_amount;
    }

    public BigDecimal getActivity_reduce_amount() {
        return activity_reduce_amount;
    }

    public void setActivity_reduce_amount(BigDecimal activity_reduce_amount) {
        this.activity_reduce_amount = activity_reduce_amount;
    }

    public BigDecimal getCoupon_reduce_amount() {
        return coupon_reduce_amount;
    }

    public void setCoupon_reduce_amount(BigDecimal coupon_reduce_amount) {
        this.coupon_reduce_amount = coupon_reduce_amount;
    }

    public BigDecimal getOriginal_total_amount() {
        return original_total_amount;
    }

    public void setOriginal_total_amount(BigDecimal original_total_amount) {
        this.original_total_amount = original_total_amount;
    }

    public BigDecimal getFeight_fee() {
        return feight_fee;
    }

    public void setFeight_fee(BigDecimal feight_fee) {
        this.feight_fee = feight_fee;
    }

    public String getExpire_time() {
        return expire_time;
    }

    public void setExpire_time(String expire_time) {
        this.expire_time = expire_time;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getOperate_time() {
        return operate_time;
    }

    public void setOperate_time(String operate_time) {
        this.operate_time = operate_time;
    }

    public String getCreate_date() {
        return create_date;
    }

    public void setCreate_date(String create_date) {
        this.create_date = create_date;
    }

    public String getCreate_hour() {
        return create_hour;
    }

    public void setCreate_hour(String create_hour) {
        this.create_hour = create_hour;
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
        OrderInfo orderInfo = (OrderInfo) o;
        return Objects.equals(id, orderInfo.id) && Objects.equals(province_id, orderInfo.province_id) && Objects.equals(order_status, orderInfo.order_status) && Objects.equals(user_id, orderInfo.user_id) && Objects.equals(total_amount, orderInfo.total_amount) && Objects.equals(activity_reduce_amount, orderInfo.activity_reduce_amount) && Objects.equals(coupon_reduce_amount, orderInfo.coupon_reduce_amount) && Objects.equals(original_total_amount, orderInfo.original_total_amount) && Objects.equals(feight_fee, orderInfo.feight_fee) && Objects.equals(expire_time, orderInfo.expire_time) && Objects.equals(create_time, orderInfo.create_time) && Objects.equals(operate_time, orderInfo.operate_time) && Objects.equals(create_date, orderInfo.create_date) && Objects.equals(create_hour, orderInfo.create_hour) && Objects.equals(create_ts, orderInfo.create_ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, province_id, order_status, user_id, total_amount, activity_reduce_amount, coupon_reduce_amount, original_total_amount, feight_fee, expire_time, create_time, operate_time, create_date, create_hour, create_ts);
    }

    @Override
    public String toString() {
        return "OrderInfo{" +
                "id=" + id +
                ", province_id=" + province_id +
                ", order_status='" + order_status + '\'' +
                ", user_id=" + user_id +
                ", total_amount=" + total_amount +
                ", activity_reduce_amount=" + activity_reduce_amount +
                ", coupon_reduce_amount=" + coupon_reduce_amount +
                ", original_total_amount=" + original_total_amount +
                ", feight_fee=" + feight_fee +
                ", expire_time='" + expire_time + '\'' +
                ", create_time='" + create_time + '\'' +
                ", operate_time='" + operate_time + '\'' +
                ", create_date='" + create_date + '\'' +
                ", create_hour='" + create_hour + '\'' +
                ", create_ts=" + create_ts +
                '}';
    }
}

