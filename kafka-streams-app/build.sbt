ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-streams-app",
    libraryDependencies ++= Seq(
      "org.apache.kafka"  % "kafka-streams"       % "4.3.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "4.3.0",
      "io.circe"         %% "circe-core"          % "0.14.16",
      "io.circe"         %% "circe-generic"       % "0.14.16",
      "io.circe"         %% "circe-parser"        % "0.14.16",
      "ch.qos.logback"    % "logback-classic"     % "1.5.18",
    ),
    Compile / run / fork := true,
  )
