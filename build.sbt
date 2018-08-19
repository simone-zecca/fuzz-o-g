
name := "fuzz-ography"

version := "0.1"

scalaVersion := "2.11.11"

lazy val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark"       %% "spark-core"                % sparkVersion   % "provided",
  "org.apache.spark"       %% "spark-sql"                 % sparkVersion   % "provided",
  "org.apache.spark"       %% "spark-hive"                % sparkVersion   % "provided",
  "org.scalatest"          %% "scalatest"                 % "3.0.5"        % "test",
  ("com.github.pureconfig" %% "pureconfig"                % "0.8.0")
    .exclude(org = "xml-apis", name = "xml-apis"),
  "xml-apis"               %  "xml-apis"                  % "1.4.01"
)
isSnapshot := true
