name := "spark-engine"
version := "0.0.1"
scalaVersion := "2.12.11"

val sparkVersion = "3.1.2"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "com.github.scopt" %% "scopt" % "4.0.0-RC2",
    "org.scala-lang" % "scala-reflect" % "2.12.11"
)

mainClass in assembly := Some("Driver.MainApp")
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
        case x => MergeStrategy.first
}
