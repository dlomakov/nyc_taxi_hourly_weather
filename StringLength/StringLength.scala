object StringLength {
  def main(args: Array[String]): Unit ={
    println("Введите строку:")
    val inputString = scala.io.StdIn.readLine()
    println(s"Длина строки: ${inputString.length}")
  }
}
