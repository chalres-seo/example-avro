package com.example.avro

import org.apache.avro.generic.GenericData
import org.hamcrest.CoreMatchers._
import org.joda.time.DateTime
import org.junit.{Assert, Test}

class TestAvroGenericRecordFactory {

  @Test
  def testAvroRecord(): Unit = {
    val nameSpace: String = "sample"
    val name: String = "test"

    val dateTime1: String = DateTime.now().toString
    val dateTime2: String = DateTime.now().toString

    val oldRecordValues: Map[String, String] = Map(
      "dateTime" -> dateTime1,
      "uid" -> "uid-1",
      "name" -> "name-1"
    )
    val recordValues: Map[String, String] = Map(
      "dateTime" -> dateTime2,
      "uid" -> "uid-2",
      "name" -> "name-2",
      "email" -> "email-2"
    )

    val oldAvroRecord: GenericData.Record = AvroGenericRecordFactory.createGenericRecord(nameSpace, name, oldRecordValues, Option(1))
    Assert.assertThat(oldAvroRecord.get("dateTime").toString, is(dateTime1))
    Assert.assertThat(oldAvroRecord.get("uid").toString, is("uid-1"))
    Assert.assertThat(oldAvroRecord.get("name").toString, is("name-1"))


    val avroRecord = AvroGenericRecordFactory.createGenericRecord(nameSpace, name, recordValues)
    Assert.assertThat(avroRecord.get("dateTime").toString, is(dateTime2))
    Assert.assertThat(avroRecord.get("uid").toString, is("uid-2"))
    Assert.assertThat(avroRecord.get("name").toString, is("name-2"))
    Assert.assertThat(avroRecord.get("email").toString, is("email-2"))
    Assert.assertNull(avroRecord.get("phone"))

    val revOldAvroRecord = AvroGenericRecordFactory.createGenericRecord(nameSpace, name, oldRecordValues)
    Assert.assertThat(revOldAvroRecord.get("dateTime").toString, is(dateTime1))
    Assert.assertThat(revOldAvroRecord.get("uid").toString, is("uid-1"))
    Assert.assertThat(revOldAvroRecord.get("name").toString, is("name-1"))
    Assert.assertThat(revOldAvroRecord.get("email").toString, is("null"))
    Assert.assertNull(avroRecord.get("phone"))
  }
}
