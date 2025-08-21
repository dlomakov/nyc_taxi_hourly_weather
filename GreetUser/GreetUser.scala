object GreetUser {
  def main (args: Array[String]):
  Unit = {
    println("Введите Ваше имя:")
    val name = scala.io.StdIn.readLine()
    println(s"Привет, $name!")
  }
}