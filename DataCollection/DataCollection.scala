import scala.language.postfixOps

object DataCollection {
  def main(args: Array[String]): Unit = {
    // 1. Объявление переменных
    val age: Int = 25
    val weight: Double = 68.5
    val name: String = "Иван"
    val isStudent: Boolean = true

    // Вывод значений переменных
    println(s"Имя: $name")
    println(s"Возраст: $age")
    println(s"Вес: $weight")
    println(s"Студент: $isStudent")

    // 2. Функция сложения двух чисел
    def addNumbers(a: Int, b: Int): Int = a + b

    // 3.1. Вызов функции сложения
    val sum = addNumbers(5, 7)
    println(s"Сумма чисел: $sum")

    // 4. Функция определения возрастной категории
    def ageCategory(age: Int): String = {
      if (age < 30) "Молодой" else "Взрослый"
    }

    // 4.1. Вызов функции возрастной категории
    val category = ageCategory(age)
    println(s"Возрастная категория: $category")

    // 5. Цикл от 1 до 10
    println("Числа от 1 до 10:")
    for (i <- 1 to 10) {
      println(i)
    }

    // 5.1. Список имен студентов
    val students = List("Анна", "Петр", "Мария", "Сергей")
    println("Список студентов:")
    for (student <- students) {
      println(student)
    }

    // 6. Программа с пользовательским вводом
    println("\nВведите ваше имя:")
    val userName = scala.io.StdIn.readLine()

    println("Введите ваш возраст:")
    val userAge = scala.io.StdIn.readInt()

    println("Вы студент? (да/нет):")
    val userStatusInput = scala.io.StdIn.readLine()
    val userIsStudent = userStatusInput.toLowerCase == "да"

    // Вывод информации о пользователе
    println("\nИнформация о пользователе:")
    println(s"Имя: $userName")
    println(s"Возраст: $userAge")
    println(s"Студент: $userIsStudent")
    println(s"Возрастная категория: ${ageCategory(userAge)}")

    // 7. For comprehension
    val numbers = 1 to 10 toList

    // Квадраты чисел
    val squares = for (n <- numbers) yield n * n
    println(s"\nКвадраты чисел: $squares")

    // Четные числа
    val evenNumbers = for {
      n <- numbers
      if n % 2 == 0
    } yield n
    println(s"Четные числа: $evenNumbers")
  }
}