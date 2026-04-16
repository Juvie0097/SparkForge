ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.sparkforge"
ThisBuild / version      := "0.1.0"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "sparkforge",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"      % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"       % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive"      % sparkVersion % Provided,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
      "com.typesafe"      % "config"          % "1.4.3",
      "org.scalatest"    %% "scalatest"       % "3.2.18"     % Test
    ),

    // Spark 3.5 still emits some Java 11+ reflective access warnings; quiet them
    Compile / javacOptions ++= Seq("-source", "11", "-target", "11"),
    Compile / scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint:_",
      "-Ywarn-unused"
    ),

    // Tests need an in-process Spark; allow reflection
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    ),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case _                             => MergeStrategy.first
    },
    assembly / assemblyJarName := s"sparkforge-${version.value}.jar",
    // Spark provides these on the cluster; do not bundle.
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter { f =>
        val n = f.data.getName
        n.startsWith("spark-") || n.startsWith("scala-")
      }
    }
  )
