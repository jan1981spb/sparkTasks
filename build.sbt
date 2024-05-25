ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

libraryDependencies += "com.google.code.findbugs" % "jsr305" % "3.0.2"
libraryDependencies += "org.scalafx" %% "scalafx" % "22.0.0-R33"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"
// https://mvnrepository.com/artifact/org.scalafx/scalafx



lazy val root = (project in file("."))
  .settings(
    name := "sparkSandbox"
  )
