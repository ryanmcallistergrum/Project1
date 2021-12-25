ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.2"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "com.lihaoyi" %% "ujson" % "1.4.3"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.10"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"
libraryDependencies += "com.outr" %% "hasher" % "1.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "Project1",
    version := "0.1",
    scalaVersion := "2.12.15",
    mainClass := Some("Main")
  )

assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}