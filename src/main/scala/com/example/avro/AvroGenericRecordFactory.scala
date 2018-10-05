package com.example.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecordBuilder

object AvroGenericRecordFactory {
  def createGenericRecord(schemaNameSpace: String,
                          schemaName: String,
                          values: Map[String, Any],
                          schemaVersion: Option[Int]): Record = {

    val schema: Option[Schema] = if (schemaVersion.isEmpty) {
      AvroSchemaStore.getLatestSchema(schemaNameSpace, schemaName)
    } else {
      AvroSchemaStore.getSchema(schemaNameSpace, schemaName, schemaVersion.get)
    }

    val recordBuilder = new GenericRecordBuilder(schema.get)

    values.foreach(value => recordBuilder.set(value._1, value._2))
    recordBuilder.build()
  }

  def createGenericRecord(schemaNameSpace: String, schemaName: String, values: Map[String, Any]): Record = {
    this.createGenericRecord(schemaNameSpace, schemaName, values, None)
  }

  def createGenericRecord(schema: Schema, values: Map[String, Any]): Record = {
    this.createGenericRecord(schema.getNamespace, schema.getName, values, None)
  }
}
