/**
  * Created by zhongxl on 2016/9/28.
  */

object Scala_learn_tst {
  def main(args: Array[String]) {
    val colors = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")

    colors.keys.foreach { i => // foreach 函数中 i 就是 key
      print("Key = " + i)
      println(" Value = " + colors(i))
    }
    colors.foreach(i => // foreach 函数中 i 就是 key和value的集合
      println("Key = " + i._1 + "value=" + i._2))
    colors.foreach((kv: (String, String)) =>
      println("Key = " + kv._1 + " " + "value=" + kv._2))
    println("colors_red:" + colors("red")) //返回对应map的值，类似于上面的colors(i)
    val l = List(1, 2, 4)
    val ll = l.map(x => x * 2 + 1)
    val l2 = l.map(x => change(x))
    val l3 = l.map(x => change2(x))
    println(change(2))
    println("ll:" + ll)
    println("l2:" + l2)
    println("l3:" + l3)
    println("l4:"+change3(2))
    println("sum:"+addfunc(1,3))
    val l4=l.map(x=> addfunc(x,2))
    val l42=l.map(x=> addfunc(x,x*x))
    println("addfunc+2:"+l42)
    //函数按照名称调用
    delayed(time());
    //调用递归函数
    for (i<-1 to 10){
      println("FACTORIAL OF "+i+":="+factorial(i))
    }
    def tst(a:Int=5,b:Int=7,args:List[Object])={//泛型，由于此处是打印操作，所以可以是String.如果是其他的操作则必须规定好类型
      var i:Int=0
      var sum=0
      for (arg<-args){
        println("测试"+i+":"+arg.toString())
        i+=1
      }
      sum=a+b
      sum
    }
//    def tst(a:Int=5,b:Int=7,args:List[String])={//泛型，由于此处是打印操作，所以可以是String.如果是其他的操作则必须规定好类型
//    var i:Int=0
//      var sum=0
//      for (arg<-args){
//        println("测试"+i+":"+arg)
//        i+=1
//      }
//      sum=a+b
//      sum
//    }

    tst(a=4,args=List("Hello", "Scala", "Python"))

  }

  def change(x: BigInt): BigInt = {
    return x.pow(2)
  }
  def change2(x: Int) = {
    if (x > 3) 3 else if (x == 0) 1 else x
  }
  def change22(x: Int) = {
    if (x > 3) {
      3
    } else {
      x
    }
  }
  def change3(x:Int): Int={
    var a=0;//首先需要定义，因为后面返回了该值，否则不需要定义
    for(a<- 1 to 10){  //注意for循环 a<- 后面可以传list，array，也可以是类似于Python 中的 len()
      if (a<x){
      println("a:"+a)
      }
    }
    return a
  }
  def addfunc(x:Int,y:Int):Int={
    var sum=0;
    sum=x+y
    return sum
  }
  //下面是函数名调用的函数
  def delayed(t:Long)={
    println("In delayded method")
    println("Param:"+t)
    t
  }
  def time()={
    println("Getting time")
    System.nanoTime()
  }
  //递归函数
  def factorial(n: BigInt): BigInt = {
    if (n <= 1)
      1
    else
      n * factorial(n - 1)
  }

}