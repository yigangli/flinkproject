import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyg.realtime.bean.OrderWide
import org.apache.commons.beanutils.BeanUtils

import scala.collection.mutable

/**
 * @author: yigang
 * @date: 2021/4/1
 * @desc:
 */
object Test1 {
    def main(args: Array[String]): Unit = {
        val a = """{"activity_reduce_amount":0.00,"category3_id":803,"category3_name":"米面杂粮","coupon_reduce_amount":0.00,"create_time":"2021-05-07 16:02:37","detail_id":92970,"feight_fee":7.00,"order_id":32732,"order_price":11.00,"order_status":"1001","original_total_amount":58819.00,"province_3166_2_code":"CN-TJ","province_area_code":"120000","province_id":2,"province_iso_code":"CN-12","province_name":"天津","sku_id":24,"sku_name":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g","sku_num":3,"split_total_amount":33.00,"spu_id":8,"spu_name":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g","tm_id":7,"tm_name":"金沙河","total_amount":58826.00,"user_age":38,"user_gender":"F","user_id":10}"""

        println(a)
        val wide: OrderWide = JSON.parseObject(a, classOf[OrderWide])
        println(wide)
    }
}