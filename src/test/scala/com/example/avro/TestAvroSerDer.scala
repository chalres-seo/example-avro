package com.example.avro

import java.nio.ByteBuffer

import com.example.record.SampleRecords
import com.example.record.SampleRecords.{UserRecordv1, UserRecordv2}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecordBuilder}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.reflect.{ReflectDatumReader, ReflectDatumWriter}
import org.apache.avro.util.{ByteBufferOutputStream, ReusableByteBufferInputStream}
import org.hamcrest.CoreMatchers._
import org.joda.time.DateTime
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._
import scala.util.Try

class TestAvroSerDer {

  @Test
  def testAvroGenericRecordSerDer(): Unit = {
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
    val avroRecord = AvroGenericRecordFactory.createGenericRecord(nameSpace, name, recordValues)
    val revOldAvroRecord = AvroGenericRecordFactory.createGenericRecord(nameSpace, name, oldRecordValues)

    val serRecords = Vector(oldAvroRecord, avroRecord, revOldAvroRecord).map(AvroSerDer.genericRecordSer)

    val derRecords: Vector[GenericData.Record] = serRecords.map(AvroSerDer.genericRecordDer)

    Assert.assertThat(derRecords(0).get("dateTime").toString, is(dateTime1))
    Assert.assertThat(derRecords(0).get("uid").toString, is("uid-1"))
    Assert.assertThat(derRecords(0).get("name").toString, is("name-1"))
    Assert.assertThat(derRecords(0).get("email").toString, is("null"))
    Assert.assertNull(derRecords(2).get("phone"))

    Assert.assertThat(derRecords(1).get("dateTime").toString, is(dateTime2))
    Assert.assertThat(derRecords(1).get("uid").toString, is("uid-2"))
    Assert.assertThat(derRecords(1).get("name").toString, is("name-2"))
    Assert.assertThat(derRecords(1).get("email").toString, is("email-2"))
    Assert.assertNull(derRecords(2).get("phone"))

    Assert.assertThat(derRecords(2).get("dateTime").toString, is(dateTime1))
    Assert.assertThat(derRecords(2).get("uid").toString, is("uid-1"))
    Assert.assertThat(derRecords(2).get("name").toString, is("name-1"))
    Assert.assertThat(derRecords(2).get("email").toString, is("null"))
    Assert.assertNull(derRecords(2).get("phone"))
  }

  @Test
  def testAvroReflectRecordSerDer(): Unit = {
    val dateTime1: String = DateTime.now().toString
    val dateTime2: String = DateTime.now().toString

    val userRecordv1 = SampleRecords.UserRecordv1(dateTime1, "uid-1", "name-1")
    val userRecordv2 = SampleRecords.UserRecordv2(dateTime2, "uid-2", "name-2", "email-2")
    val reuseRecordv1 = SampleRecords.UserRecordv1.getReuseRecord
    val reuseRecordv2 = SampleRecords.UserRecordv2.getReuseRecord

    val serRecord: ByteBuffer = AvroSerDer.reflectSer[UserRecordv1](SampleRecords.UserRecordv1.getDatumWriter, userRecordv1)
    val derRecord: UserRecordv1 = AvroSerDer.reflectDer[UserRecordv1](SampleRecords.UserRecordv1.getDatumReader, serRecord, reuseRecordv1)

    Assert.assertThat(derRecord, is(userRecordv1))

    val serRecordv2: ByteBuffer = AvroSerDer.reflectSer[UserRecordv2](SampleRecords.UserRecordv2.getDatumWriter, userRecordv2)
    val derRecordv2: UserRecordv2 = AvroSerDer.reflectDer[UserRecordv2](SampleRecords.UserRecordv2.getDatumReader, serRecordv2, reuseRecordv2)

    Assert.assertThat(derRecordv2, is(userRecordv2))

    val revRecord = Try(AvroSerDer.reflectDer[UserRecordv2](SampleRecords.UserRecordv2.getDatumReader, serRecord, reuseRecordv2))
    Assert.assertTrue(revRecord.isFailure)
  }

    @Test
  def testGenericDatum(): Unit = {
    val schema = AvroSchemaStore.getLatestSchema("sample", "test").get
    val genericRecordBuilder = new GenericRecordBuilder(schema)

    genericRecordBuilder
      .set("dateTime", DateTime.now().toString)
      .set("uid", "uid-1")
      .set("name", "name-1")

    val genericRecord1 = genericRecordBuilder.build()

    schema.getFields.foreach(genericRecordBuilder.clear)
    genericRecordBuilder
      .set("dateTime", DateTime.now().toString)
      .set("uid", "uid-2")
      .set("name", "name-2")

    val genericRecord2 = genericRecordBuilder.build()

    val outputStream = new ByteBufferOutputStream()
    val binaryEncoder = EncoderFactory.get.blockingBinaryEncoder(outputStream, null)

    val datumWriter =  new GenericDatumWriter[GenericData.Record]()
    datumWriter.setSchema(schema)

    val inputStream = new ReusableByteBufferInputStream()
    val binaryDecoder = DecoderFactory.get.binaryDecoder(inputStream, null)
    val datumReader =  new GenericDatumReader[GenericData.Record](schema, schema)

    // ser
    datumWriter.write(genericRecord1, binaryEncoder)
    binaryEncoder.flush()

    val buffers = outputStream.getBufferList
    val result = ByteBuffer.allocate(buffers.map(_.remaining()).sum)

    buffers.foreach(result.put)
    result.flip()

    // der
    result.rewind()
    inputStream.setByteBuffer(result)

    val tmpUserRecord = new GenericData.Record(schema)
    datumReader.read(tmpUserRecord, binaryDecoder)

    println(datumReader.getSchema)
    println(ByteBuffer.wrap(datumReader.getSchema.toString.getBytes))
    println(genericRecord1)
    println(genericRecord2)
    println(result)
    println(tmpUserRecord)
  }

  @Test
  def testReflectDatum(): Unit = {
    // setup
    val userRecord1 = SampleRecords.UserRecord(DateTime.now().toString, "uid-1", "name-1")
    val userRecord2 = SampleRecords.UserRecord(DateTime.now().toString, "uid-2", "name-2")

    val outputStream = new ByteBufferOutputStream()
    val binaryEncoder = EncoderFactory.get.blockingBinaryEncoder(outputStream, null)
    val datumWriter =  new ReflectDatumWriter(classOf[SampleRecords.UserRecord])

    val inputStream = new ReusableByteBufferInputStream()
    val binaryDecoder = DecoderFactory.get.binaryDecoder(inputStream, null)
    val datumReader =  new ReflectDatumReader(classOf[SampleRecords.UserRecord])

    // ser
    datumWriter.write(userRecord1, binaryEncoder)
    binaryEncoder.flush()

    val buffers = outputStream.getBufferList
    val result = ByteBuffer.allocate(buffers.map(_.remaining()).sum)

    buffers.foreach(result.put)
    result.flip()

    // der
    result.rewind()
    inputStream.setByteBuffer(result)

    val tmpUserRecord = SampleRecords.UserRecord(DateTime.now().toString, "", "")
    datumReader.read(tmpUserRecord, binaryDecoder)

    println(datumReader.getSchema)
    println(ByteBuffer.wrap(datumReader.getSchema.toString.getBytes))
    println(userRecord1)
    println(userRecord2)
    println(result)
    println(tmpUserRecord)
  }

}
