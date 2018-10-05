package com.example.utils

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

object AppConfig extends LazyLogging {
  private val applicationConfigFilePath = "conf/application.conf"
  private val conf: Config = this.readConfigFile(applicationConfigFilePath)
  logger.debug(s"applicaion conf: ${conf.toString}")

  def getApplicationName: String = conf.getString("application.name")

  private def readConfigFile(confFilePath: String): Config = {
    logger.debug(s"read config from file. path: $confFilePath")
    ConfigFactory.parseFile(new File(confFilePath)).resolve()
  }

  def getAvroSchemaFileRootPath: String = conf.getString("avro.schema.root.path")
}