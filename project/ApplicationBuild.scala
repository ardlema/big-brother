import sbt.Keys._
import sbt._

object ApplicationBuild extends Build {

  Resolver.sonatypeRepo("releases")

  import org.scalastyle.sbt.ScalastylePlugin.{Settings => scalastyleSettings}
  import scoverage.ScoverageSbtPlugin.{buildSettings => scoverageSettings}

  object Versions {
    val spark = "1.3.0"
  }

  val customScalacOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen")

  val customJavaInRuntimeOptions = Seq(
    "-Xmx512m"
  )

  val customJavaInTestOptions = Seq(
    "-Xmx512m"
  )

  val customResolvers = Seq(
    Classpaths.sbtPluginReleases,
    Classpaths.typesafeReleases,
    "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
  )

  val customLibraryDependencies = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-streaming" % Versions.spark,
    "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.0",
    "org.apache.curator" % "curator-test" % "2.7.1",
    "org.slf4j" % "slf4j-api" % "1.7.10",
    "ch.qos.logback" % "logback-classic" % "1.1.2",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  ).map(_.exclude(
    "org.slf4j", "slf4j-log4j12"
  ))

  lazy val main = Project(
    id = "big-brother",
    base = file("."),
    settings = Seq(
      version := "1.0",
      scalaVersion := "2.10.4",
      javaOptions in Runtime ++= customJavaInRuntimeOptions,
      javaOptions in Test ++= customJavaInTestOptions,
      scalacOptions ++= customScalacOptions,
      resolvers ++= customResolvers,
      libraryDependencies ++= customLibraryDependencies,
      parallelExecution in Test := false,
      fork in Test := true
    ) ++ scalastyleSettings ++ scoverageSettings
  )

}
