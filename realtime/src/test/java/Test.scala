import scala.collection.mutable

/**
 * @author: yigang
 * @date: 2021/4/1
 * @desc:
 */
object Test {
  def main(args: Array[String]): Unit = {
    val set = mutable.Set[String]()
    println(set.add("2"))
    println(set.add("1"))
    println(set.add("1"))
  }
}
