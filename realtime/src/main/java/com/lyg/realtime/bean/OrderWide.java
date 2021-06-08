package com.lyg.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;

import java.math.BigDecimal;

/**
 * Author: Felix
 * Date: 2021/2/5
 * Desc: 订单宽表实体类
 */
@Data
@AllArgsConstructor
public class OrderWide {
    Long detail_id;
    Long order_id ;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;
    Long province_id;
    String order_status;
    Long user_id;

    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    BigDecimal split_feight_fee;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    BigDecimal split_total_amount;

    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;

    String province_name;//查询维表得到
    String province_area_code;
    String province_iso_code;
    String province_3166_2_code;

    Integer user_age ;
    String user_gender;

    Long spu_id;     //作为维度数据 要关联进来
    Long tm_id;
    Long category3_id;
    String spu_name;
    String tm_name;
    String category3_name;

    public OrderWide(OrderInfo orderInfo, OrderDetail orderDetail){
        mergeOrderInfo(orderInfo);
        mergeOrderDetail(orderDetail);

    }

    public void  mergeOrderInfo(OrderInfo orderInfo  )  {
        if (orderInfo != null) {
            this.order_id = orderInfo.id;
            this.order_status = orderInfo.order_status;
            this.create_time = orderInfo.create_time;
            this.create_date = orderInfo.create_date;
            this.activity_reduce_amount = orderInfo.activity_reduce_amount;
            this.coupon_reduce_amount = orderInfo.coupon_reduce_amount;
            this.original_total_amount = orderInfo.original_total_amount;
            this.feight_fee = orderInfo.feight_fee;
            this.total_amount =  orderInfo.total_amount;
            this.province_id = orderInfo.province_id;
            this.user_id = orderInfo.user_id;
        }
    }

    public void mergeOrderDetail(OrderDetail orderDetail  )  {
        if (orderDetail != null) {
            this.detail_id = orderDetail.id;
            this.sku_id = orderDetail.sku_id;
            this.sku_name = orderDetail.sku_name;
            this.order_price = orderDetail.order_price;
            this.sku_num = orderDetail.sku_num;
            this.split_activity_amount=orderDetail.split_activity_amount;
            this.split_coupon_amount=orderDetail.split_coupon_amount;
            this.split_total_amount=orderDetail.split_total_amount;
        }
    }

    public void mergeOtherOrderWide(OrderWide otherOrderWide){
        this.order_status = ObjectUtils.firstNonNull( this.order_status ,otherOrderWide.order_status);
        this.create_time =  ObjectUtils.firstNonNull(this.create_time,otherOrderWide.create_time);
        this.create_date =  ObjectUtils.firstNonNull(this.create_date,otherOrderWide.create_date);
        this.coupon_reduce_amount =  ObjectUtils.firstNonNull(this.coupon_reduce_amount,otherOrderWide.coupon_reduce_amount);
        this.activity_reduce_amount =  ObjectUtils.firstNonNull(this.activity_reduce_amount,otherOrderWide.activity_reduce_amount);
        this.original_total_amount =  ObjectUtils.firstNonNull(this.original_total_amount,otherOrderWide.original_total_amount);
        this.feight_fee = ObjectUtils.firstNonNull( this.feight_fee,otherOrderWide.feight_fee);
        this.total_amount =  ObjectUtils.firstNonNull( this.total_amount,otherOrderWide.total_amount);
        this.user_id =  ObjectUtils.<Long>firstNonNull(this.user_id,otherOrderWide.user_id);
        this.sku_id = ObjectUtils.firstNonNull( this.sku_id,otherOrderWide.sku_id);
        this.sku_name =  ObjectUtils.firstNonNull(this.sku_name,otherOrderWide.sku_name);
        this.order_price =  ObjectUtils.firstNonNull(this.order_price,otherOrderWide.order_price);
        this.sku_num = ObjectUtils.firstNonNull( this.sku_num,otherOrderWide.sku_num);
        this.split_activity_amount=ObjectUtils.firstNonNull(this.split_activity_amount);
        this.split_coupon_amount=ObjectUtils.firstNonNull(this.split_coupon_amount);
        this.split_total_amount=ObjectUtils.firstNonNull(this.split_total_amount);
    }

    @Override
    public String toString() {
        return "OrderWide{" +
                "detail_id=" + detail_id +
                ", order_id=" + order_id +
                ", sku_id=" + sku_id +
                ", order_price=" + order_price +
                ", sku_num=" + sku_num +
                ", sku_name='" + sku_name + '\'' +
                ", province_id=" + province_id +
                ", order_status='" + order_status + '\'' +
                ", user_id=" + user_id +
                ", total_amount=" + total_amount +
                ", activity_reduce_amount=" + activity_reduce_amount +
                ", coupon_reduce_amount=" + coupon_reduce_amount +
                ", original_total_amount=" + original_total_amount +
                ", feight_fee=" + feight_fee +
                ", split_feight_fee=" + split_feight_fee +
                ", split_activity_amount=" + split_activity_amount +
                ", split_coupon_amount=" + split_coupon_amount +
                ", split_total_amount=" + split_total_amount +
                ", expire_time='" + expire_time + '\'' +
                ", create_time='" + create_time + '\'' +
                ", operate_time='" + operate_time + '\'' +
                ", create_date='" + create_date + '\'' +
                ", create_hour='" + create_hour + '\'' +
                ", province_name='" + province_name + '\'' +
                ", province_area_code='" + province_area_code + '\'' +
                ", province_iso_code='" + province_iso_code + '\'' +
                ", province_3166_2_code='" + province_3166_2_code + '\'' +
                ", user_age=" + user_age +
                ", user_gender='" + user_gender + '\'' +
                ", spu_id=" + spu_id +
                ", tm_id=" + tm_id +
                ", category3_id=" + category3_id +
                ", spu_name='" + spu_name + '\'' +
                ", tm_name='" + tm_name + '\'' +
                ", category3_name='" + category3_name + '\'' +
                '}';
    }
}

