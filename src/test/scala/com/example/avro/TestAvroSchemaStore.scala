package com.example.avro

import java.nio.ByteBuffer

import com.example.record.SampleRecords
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.reflect.{ReflectDatumReader, ReflectDatumWriter}
import org.apache.avro.util.{ByteBufferOutputStream, ReusableByteBufferInputStream}
import org.joda.time.DateTime
import org.junit.{Assert, Test}

import scala.collection.JavaConversions._

class TestAvroSchemaStore {
  private val testNameSpace = "sample"
  private val testName = "test"

  @Test
  def testAvroSchemaStore(): Unit = {
    AvroSchemaStore.getAllStoreinfo.foreach(println)
    Assert.assertTrue(AvroSchemaStore.getLatestSchema(testNameSpace, testName).isDefined)
  }

  @Test
  def testCreateSchema(): Unit = {
    // setup
    val userRecord1 = SampleRecords.UserRecord(DateTime.now().toString, "uid-1", "email-1")
    val userRecord2 = SampleRecords.UserRecord(DateTime.now().toString, "uid-2", "email-2")

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

    println(result)

    // der
    result.rewind()
    inputStream.setByteBuffer(result)

    val tmpUserRecord = SampleRecords.UserRecord(DateTime.now().toString, "", "")
    datumReader.read(tmpUserRecord, binaryDecoder)

    println(userRecord1)
    println(userRecord2)
    println(tmpUserRecord)
  }
}
