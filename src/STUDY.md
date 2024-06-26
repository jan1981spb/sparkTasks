1.Структура задания, логический и физический планы выполнения
Задание включает этапы по вертикали
Границы между этапами - операции с широкими преобразованиями.
Каждый этап включает задачи - по горизонтали

Задачи распределяются по исполнителям - одна задача может выполняться
на нескольких исполнителях.

2.RDD vs DataFrame vs DataSet
  RDD может создана из коллекции: SparkSession.parallelize
  DataFrame имеет схему, в отличие от RDD.  Используется Tungstren
    Tungstren
             - Не используется сериализация Java
             - Сериализованные объекты занимают меньше места
             - Объекты могут помещаются вне кучи - экономия на сборке мусора
             - Ускоряются SQL-запросы за счет оптимизации сортировки и хэширования
             - Более быстрая генерация JavaBytecode
  DataSet
             Схема на этапе компилляции. Используется Catalyst
    Catalyst:
            - Логический план запроса предстляет собой дерево (атрибутов и отношений)
            - Используется оптимизация логического плана
            - Оптимизация генерации кода
3.Узкие и широкие преобразования
  Широкие - родительской RDD соответствует более дочерней.
            И дочерние RDD заранее не определены
            Предполагают перетасовку
  Узкие - родительской RDD соответствует НЕ более дочерней.
          Но одна дочерняя может соответствовать более чем одной родительской
          Все дочерние RDD заранее могут быть определены
  repartition - влечет перетасовку
  colease - при уменьшении количества партиций не влечет перетасовку,
            при увелиении - влечет
            (В результате уменьшения количества партиций одна
            дочерняя RDD может начать соответсвовать более чем одной родительской.
            Что соответствует узкому преобразованию)
4. Действия и трансформации
            Трансформации:
            - Один набор: map, flatMap, filter, distinct
            - 2 набора: union, intersection, substract, cartesian
            Дейтствия:
            collect, count, take(n), saveAsText, saveAsSequence, forEach, reduce,
            fold, aggregate
5.Трансформации, ведущие к перемешиванию:
            sort, reduceByKey, sortByKey, cogroup, join, groupByKey, combineByKey

6.Работа с парами ключ-значение:
            reduceByKey, groupByKey, combineByKey, keys, values, sortByKey, mapValues,
            subtractByKey, join, cogroup

            combineByKey - в отличие от reduceByKey позволяет получить результат другого
                        типа (отличного от типа ключей)
            groupByKey - каждому ключу ставит в соответствие итератор.
            Проблема аггрегаторов combineByKey, groupByKey, reduceByKey : результат аггрегирования
            помещается в узел исполнителя, соответствующий определенному ключу. И может оказаться
            слишком большим для этого ключа
7.SparkSQL
            Соединение
            df1.join(df2, df1("key") === df2("key"), "inner")
8.Форматы данных. Источники.
             parquet, csv, json, текстовые, sequenceFile, jdbc, cassandra
9.Join и вопросы производительности
             broadcast - для помещения малого RDD полностью в оперативную память узла.перетасовку
             Рекомендуется сужать объем RDD, перед их джойном
             Если есть повторяющиеся ключи, лучше перед джойном сделать distinct
10.Persiste,  Cache,  Контрольные точки
             Persiste(): (По умолчанию в куче в несериализованном виде!)
                Уровни:
                - MEMORY_ONLY - только в оперативе
                - MEMORY_ONLY_SER - только в оперативе в сериализованном виде
                - MEMORY_AND_DISK - при вытеснении из оперативы, данные сохраняются на диск
                - MEMORY_AND_DISK_SER - при вытеснении из оперативы, данные сохраняются на диск в
                                        сериализованном виде
                - DISC_ONLY - сразу на диск
             Cache()  = Persiste с уровнем MEMORY_AND_DISK
             Контрольные точки: сохранение данных во внешнем хранилище.
                 Полезно в случае падения нод кластера
11. Управление партициями
              - repartition - при изменении количества партиций осуществляется полное перемешивание
                              для обеспечения равномерного распределения данных по партициям
              - colease - перемешивание при увеличении кол-ва партиций, при уменьшении - его нет.
                          Не гарантирует равномерное распределение данных по партициям.
              - partitionBy.
                     Распределяет данные по разделам директорий
                     Не гарантирует равномерного распределения.
                     В отличие от 2 предыдущих позволяет использовать
                      определенный Partitioner.
                    Виды Partitioner:
                      HashPartitioner (партиции по хэш-значению ключа)
                      RangePartitioner (партии помещаются в диапазоны равномерно распределенных
                      интервалов хэша)
                      кастомный Partitioner с набором методов numPartitions, equals, hashCode

12. Бакетирование


