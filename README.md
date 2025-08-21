## 🚕 Аналитика поездок такси NYC + погода (Scala/Spark → Hive)
Цель: собрать помесячные открытые данные поездок (NYC TLC), обогатить погодой по часам и построить витрину с почасовой статистикой по зонам города
## 📘 Описание данных
  * Информация о поездках Желтого такси (Trips): NYC TLC Yellow/Green Trip Records (parquet, за 1 месяц).
  * Справочник зон: taxi_zone_lookup.csv.
  * Погода NYC по часам: почасовой dataset. Ключ — метка часа.\
## 📊 Что делает проект
  * Загружаем NYC TLC trip records (Yellow Taxi) из HDFS
  * Чистим данные (отброс аномалий, длительность поездки, скорость, дубликаты)
  * Обогащаем справочником зон города и почасовыми погодными данными
  * Считаем агрегаты по часам и зонам:
    * число поездок
    * средняя, медианная (p50) и 95-й перцентиль длительности
    * средняя скорость (км/ч)
    * выручка
    * доля наличных платежей
    * температура и осадки
  * Строим дополнительные метрики:
    * Скользящим окном считаем 7-дневное среднее по числу поездок
    * топ-10 зон по числу поездок за день
  * Сохраняем результат в Hive таблицу (Parquet, partitioned by date)
## 🏗 Архитектура пайплайна
HDFS (raw trips, weather)\
 │\
 ▼\
Spark (Scala job)\
  ├── очистка и фильтрация\
  ├── join с зонами и погодой\
  ├── агрегации + оконные функции\
  ▼\
Hive (Parquet, dwh.fact_taxi_hourly)
## ⚙️ Технологии
  * Scala 2.11.12
  * Apache Spark 3.5.5
  * Hive 3.1.3
  * HDFS 3.3.6
  * Сборка: build sbt
## 🚀 Запуск
  * Собрать jar: sbt package
  * Запустить на YARN - пример в run.sh
  * Параметры берутся из переменных окружения:
    * TRIPS_PATH — путь к данным поездок в HDFS
    * ZONES_PATH — справочник зон
    * WEATHER_PATH — погодные данные (hourly)
    * FROM_DT / TO_DT — период анализа (например, 2024-01-01)
## 🛠️ Структура проекта
.\
├── data/&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;# датасеты\
│   └── yellow_tripdata_2025-01.parquet\
│   └── taxi_zone_lookup.csv\
│   └── nyc_weather_jan_2025.csv\
├── build.sbt&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;# конфиг сборки\
├── project/\
│   └── build.properties&emsp;&emsp;&emsp;&emsp;&emsp;# информация о версии sbt\
├── NycTaxiHourlyJob.scala&emsp;&emsp;&emsp;# основной Spark job\
├── ddl.hql&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;# Скрипты HiveQL для создания БД / таблицы под витрину\
├── run.sh&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;# Пример запуска собранной job-ы\
└── README.md
## 👨‍🏫 Примеры метрик
  * Среднее время поездки в Манхэттене в час пик (17–19): ~12 мин
  * 95-й перцентиль длительности поездок в дождь выше на 15%
  * Top-5 зон по числу поездок в сутки: Midtown, Upper East Side, JFK, LaGuardia, Downtown Brooklyn
