ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "PDSS_CW1"
  )

// Spark依赖（Scala 2.13需Spark 3.4+）
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" ,  // 核心库
  "org.apache.spark" %% "spark-sql" % "3.5.1"   // 如需SQL支持
)
