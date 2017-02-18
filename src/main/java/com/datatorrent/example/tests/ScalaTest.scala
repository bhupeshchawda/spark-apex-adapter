package com.datatorrent.example.tests

/**
  * Created by anurag on 26/1/17.
  */
class ScalaTest[T] {

}
object ScalaTest{
  def main(args: Array[String]): Unit = {
    val a=new JavaTest[Object]
    val b=a.take()
    println(a.take().map(_.asInstanceOf[Int]).getClass())
    println(a.take().headOption.getOrElse(
      println("It didn't happen")
    ))
  }
}

