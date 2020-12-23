name := "users_items"

version := "1.0"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.7"
val sparkKafkaVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"             % sparkVersion % Provided,
  "org.apache.spark"  %% "spark-sql"              % sparkVersion % Provided,
  "org.apache.spark"  %% "spark-hive"             % sparkVersion % Provided,
  "org.apache.spark"  %% "spark-sql-kafka-0-10"   % sparkKafkaVersion % Provided
)

scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-language:existentials",
  "-language:higherKinds",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
