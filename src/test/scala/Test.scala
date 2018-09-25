import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by shiyang on 2017/7/11.
  */
object Test {

  def main(args: Array[String]): Unit = {
    val buffer = new ArrayBuffer[String]()
    buffer.append("a")
    buffer.append("b")
    buffer.append("c")
    for(i<-0 until (buffer.length,2)) yield println(i)
    buffer.filter(str => str.endsWith("a")).map(str => str+"1")
    val list = new ListBuffer[String]
    list.append("a")
    list.append("b")
    list.append("c")
    try{

    }catch {
      case ex:Exception => println(ex)
    }

  }

}
