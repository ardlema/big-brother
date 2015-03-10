name := "big-brother"

organization := "org.ardlema"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.2.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.1",
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test" withSources() withJavadoc(),
  "org.scalacheck" %% "scalacheck" % "1.12.1" % "test" withSources() withJavadoc()
)

initialCommands := "import org.ardlema.bigbrother._"

