ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "com.lihaoyi" %% "ujson" % "1.4.3"

lazy val root = (project in file("."))
  .settings(
    name := "Project1",
    version := "0.1",
    scalaVersion := "2.11.12",
    mainClass := Some("Main")
  )

assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}