ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "impala_audit_spark",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1",
    libraryDependencies += "org.apache.spark" % "spark-streaming_2.13" % "3.2.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"

)
