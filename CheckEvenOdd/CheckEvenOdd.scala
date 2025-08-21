import scala.util.{Failure, Success, Try}

object CheckEvenOdd {
  def main(args: Array[String]):
    Unit = {
      println("Введите число:")
      val input = scala.io.StdIn.readLine()
      Try(input.trim.toDouble) match {
        case Success(number) => if(number.isWhole) {
          val intValue = number.toInt
          if (intValue % 2 == 0) {
            println(s"Число $number четное.")
          }
          else {
            println(s"Число $number нечетное.")
          }
        }
        else {
          println(s"Ошибка: $number не является целым числом.")
        }
        case Failure(_) => println(s"Ошибка: '$input' не является числом.'")
      }
  }
}
