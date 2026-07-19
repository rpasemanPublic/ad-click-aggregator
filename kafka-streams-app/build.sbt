ThisBuild / scalaVersion := "2.13.15"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-streams-app",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % "3.8.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "3.8.0"
    )
  )
