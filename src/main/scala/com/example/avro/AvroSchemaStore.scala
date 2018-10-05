package com.example.avro

import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ConcurrentHashMap

import com.example.utils.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.reflect.ReflectDatumReader
import org.apache.avro.{Schema, SchemaNormalization, SchemaParseException}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object AvroSchemaStore extends LazyLogging {
  case class StoreRecord(nameSpace: String, name: String, version: Int)

  private val schemaFileRootPath = AppConfig.getAvroSchemaFileRootPath
  private val schemaCache = new ConcurrentHashMap[Long, Schema]()
  private val schemaFingerprintCache = new ConcurrentHashMap[StoreRecord, Long]

  {
    init()
  }

  def getFingerprint(schema: Schema): Long = SchemaNormalization.parsingFingerprint64(schema)

  private def addSchema(schema: Schema, version: Int): Unit = {
    val storeRecord = StoreRecord(schema.getNamespace, schema.getName, version)
    val fingerprint = this.getFingerprint(schema)

    logger.info(s"add schema. schema info: $storeRecord, fingerprint: $fingerprint")

    schemaCache.put(fingerprint, schema)
    schemaFingerprintCache.put(storeRecord, fingerprint)
  }

  private def addSchema(file: File): Unit = {
    this.addSchema(AvroSchemaFactory.createSchemaFromFile(file),
      file.getName.split('.')(2).replace("v", "").toInt)
  }

  private def addSchema(path: Path): Unit = {
    this.addSchema(path.toFile)
  }

  def getSchema(nameSpace: String, name: String, version: Int): Option[Schema] = {
    logger.debug(s"find schema. name space:$nameSpace, name: $name, version: $version")
    this.getSchemaByFingerprint(schemaFingerprintCache.get(StoreRecord(nameSpace, name, version)))
  }

  def getLatestSchema(nameSpace: String, name: String): Option[Schema] = {
    logger.debug(s"find schema. name space:$nameSpace, name: $name, version: latest")
    this.getLatestSchemaVersion(nameSpace, name).flatMap { version =>
      this.getSchemaByFingerprint(schemaFingerprintCache.get(StoreRecord(nameSpace, name, version)))
    }
  }

  def getSchemaByFingerprint(fingerprint: Long): Option[Schema] = {
    logger.debug(s"find schema. fingerprint: $fingerprint")
    val schema = Option(schemaCache.get(fingerprint))

    logger.info("couldn't find schema, refresh schema store.")
    if (schema.isEmpty) refresh()

    schema
  }

  @throws(classOf[java.io.IOException])
  private def getSchemaFileList: Iterator[Path] = {
    logger.info(s"get schema file list in $schemaFileRootPath")
    Files.list(Paths.get(schemaFileRootPath)).iterator().asScala
  }

  @throws(classOf[java.io.IOException])
  private def init(): Unit = {
    logger.info("init schema store.")
    this.getSchemaFileList.foreach(this.addSchema)
    logger.info("schema list: " + schemaFingerprintCache.keys().mkString(", "))
  }

  @throws(classOf[java.io.IOException])
  def refresh(): Unit = {
    logger.info("refresh schema store.")
    schemaCache.clear()
    this.init()
  }

  def getAllStoreinfo: Vector[(StoreRecord, Schema)] = {
    logger.debug("get all schema store info.")
    schemaFingerprintCache.toVector.map { r =>
      r._1 -> schemaCache(r._2)
    }
  }

  def getSchemaVersionList(nameSpace: String, name: String): Option[Vector[Int]] = {
    logger.debug(s"get schema version list. name space: $nameSpace, name: $name")

    val versionList = schemaFingerprintCache.keys
      .filter(key => key.nameSpace == nameSpace && key.name == name)
      .map(_.version)
      .toVector

    if (versionList.isEmpty) None else Option(versionList)
  }

  def getLatestSchemaVersion(nameSpace: String, name: String): Option[Int] = {
    logger.debug(s"get latest schema version. name space: $nameSpace, name: $name")
    this.getSchemaVersionList(nameSpace, name).map(_.max)
  }

  private object AvroSchemaFactory extends LazyLogging {

    @throws(classOf[SchemaParseException])
    def createSchemaFromString(schemaDefineJsonString: String): Schema = {
      logger.debug(s"get schema from string. string: $schemaDefineJsonString")
      new Schema.Parser().parse(schemaDefineJsonString)
    }

    @throws(classOf[IOException])
    def createSchemaFromFile(file: File): Schema = {
      logger.debug(s"get schema from file. file: ${file.getAbsolutePath}")
      new Schema.Parser().parse(file)
    }

    @throws(classOf[IOException])
    def createSchemaFromFile(path: Path): Schema = {
      logger.debug(s"get schema from file. file: ${path}")
      new Schema.Parser().parse(path.toFile)
    }

    def createSchemaFromClass[T](clazz:Class[T]): Schema = {
      logger.debug(s"get schema from class reflect. class: ${clazz.getName}")
      new ReflectDatumReader(clazz).getSchema
    }
  }
}
