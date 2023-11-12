// Databricks notebook source
val sc = spark.sparkContext

val fel2021 = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/practica/world_happiness_report_2021.csv")
val fel = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/practica/world_happiness_report.csv")
fel.printSchema

fel2021.printSchema


// COMMAND ----------

import org.apache.spark.sql.functions._

    
val maxfel2021 = fel2021.select("Country name", "Ladder score")
  .orderBy(col("Ladder score").desc)  
  .head()  

 
println(s"El pais mas feliz de 2021 es $maxfel2021 ")



// COMMAND ----------

//¿Cuál es el país más “feliz” del 2021 por continente según la data?


fel2021.groupBy("Regional indicator").agg(
  first(col("Country name"))as("Pais"),
  max(col("Ladder score")).as("max_fel"),
  
).show


// COMMAND ----------

// ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?
import org.apache.spark.sql.functions._


// hayamos el pais mas feliz en 2021
val max2021 = fel2021
  .select("Country name", "Ladder score")
  .orderBy(desc("Ladder score"))
  .limit(1)

//Añadimos el año al DF
val max2021año = max2021.withColumn("año", lit("2021"))
//max2021año.printSchema
//max2021año.show

//hayamos loa valores maximos de felicidad para cada año
val felicidad = fel
  .groupBy("year")
  .agg(
    max("Life Ladder").as("life ladder"))
 //felicidad.show()


// Unimos el fel con felicidad para saber los datos completos de los paises
val union = felicidad
  .join(fel, Seq("year", "Life Ladder"), "inner")
  .orderBy("year")

//union.show()


// Creamos un nuevo DF con las columnas que queremos y casteamos año a string
val nuevoDF = union
  .withColumnRenamed("Country name", "Country name")
  .withColumnRenamed("Life Ladder", "Ladder score")
  .withColumnRenamed("year", "año")
  .withColumn("año", col("año").cast("string"))
  .select("Country name", "Ladder score", "año")

//nuevoDF.show()
//nuevoDF.printSchema

//unimos el nuevo DF al DF que creamos de 2021 y ya tenemos todos los años
val maxfelyears = nuevoDF.union(max2021año)
//maxfelyears.show

//agrupamos por paises y contamos el numero de veces que aparecen 
val contadorpaises = maxfelyears.groupBy("Country name")
.count()


contadorpaises.show






// COMMAND ----------

//¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?



// Filtramos los datos para el año 2020
val fel2020 = fel.filter(col("year") === 2020)


// Obtenenemos el país con el mayor GDP en 2020
val paisConMayorGDP = fel2020.select("Country name", "Log GDP per capita")
  .orderBy(desc("Log GDP per capita"))
  .limit(1)
  .select("Country name")
paisConMayorGDP.show

// Obtenemos df de paises y su felicidad ordenada de mayor a menor
val paisConMayorLD = fel2020.select("Country name", "Life Ladder")
  .orderBy(desc("Life Ladder"))

// añadimos una columna que indica la posicion de cada pais y renombramos la columna a posicion
val dfranked = paisConMayorLD.withColumn("row_id", monotonically_increasing_id()+1)
val renpos = dfranked.withColumnRenamed("row_id", "posicion")


// Sacar la posicion del pais indicado como mayor gpd en 2020
val pos = renpos.filter(col("Country name") === "Ireland" ).select("posicion")

pos.show


// COMMAND ----------

//¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?

// se filtra el año 2020
val fel2020 = fel.filter(col("year") === 2020)

// Calculamos los promedios de los años 2020 y 2021
val gdpPromedio2020 = fel2020.agg(avg("Log GDP per capita").as("GDP_promedio_2020")).collect()(0).getDouble(0)
val gdpPromedio2021 = fel2021.agg(avg("Logged GDP per capita").as("GDP_promedio_2021")).collect()(0).getDouble(0)

// calculamos el porcentaje de los promedios
val porcentajeVariacion = ((gdpPromedio2020 - gdpPromedio2021) / gdpPromedio2020) * 100

// establecemos dependiendo del resultado del porcentaje si aumenta o disminuye
val resultado = if (porcentajeVariacion > 0) "DISMINUYE" else "AUMENTA"


println(s" El GDP promedio $resultado. el porcentaje fue del $porcentajeVariacion%")


// COMMAND ----------

//¿Cuál es el país con mayor expectativa de vide (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia en ese indicador en el 2019?

//hayamos el pais con la maxima esperanza de vida
val maxLifeExpectancy2021 = fel2021.select("Country name", "Healthy life expectancy") 
.orderBy(desc("Healthy life expectancy"))
.limit(1)

maxLifeExpectancy2021.show

//hayamos la esperanza de vida de ese pais en 2019
val lifeExpectancy2019 = fel.filter(col("Country name") === "Singapore" && col("year") === 2019)
.select( "Healthy life expectancy at birth") as ("Healthy life expectancy in 2019")

lifeExpectancy2019.show



// COMMAND ----------


