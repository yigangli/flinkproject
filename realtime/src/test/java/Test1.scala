import com.alibaba.fastjson.JSONObject
import org.apache.commons.beanutils.BeanUtils

import scala.collection.mutable

/**
 * @author: yigang
 * @date: 2021/4/1
 * @desc:
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    val set = mutable.Set[String]()
    println(set.add("2"))
    println(set.add("1"))
    println(set.add("1"))
    var a = new JSONObject()


    BeanUtils.setProperty(a,"id","12")
    println(a.toJSONString)
  }

}
