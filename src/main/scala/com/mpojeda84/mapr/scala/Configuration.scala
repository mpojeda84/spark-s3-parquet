package com.mpojeda84.mapr.scala;

case class Configuration(source: String, target: String)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = DefaultConfiguration



  object DefaultConfiguration extends Configuration(
    "file.txt",
    "s3-file"
  )

  private val parser = new scopt.OptionParser[Configuration]("App Name") {
    head("App Name")

    opt[String]('s', "source")
      .action((s, config) => config.copy(source = s))
      .maxOccurs(1)
      .text("Input File")

    opt[String]('t', "target")
      .action((t, config) => config.copy(target = t))
      .text("S3 path to write to")

  }
}