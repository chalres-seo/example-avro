package com.example.avro

import java.nio.ByteBuffer

import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.reflect.{ReflectData, ReflectDatumReader, ReflectDatumWriter}
import org.apache.avro.util.{ByteBufferOutputStream, ReusableByteBufferInputStream}

import scala.collection.JavaConversions._

object AvroSerDer {
  private val outputStream = new ByteBufferOutputStream()
  private val inputStream = new ReusableByteBufferInputStream()

  private val binaryEncoder = EncoderFactory.get.blockingBinaryEncoder(outputStream, null)
  private val binaryDecoder = DecoderFactory.get.binaryDecoder(inputStream, null)

  private val genericRecordWriter = new GenericDatumWriter[GenericRecord]()
  private val genericRecordReader = new GenericDatumReader[GenericRecord]()

  def genericRecordSer(record: GenericData.Record): ByteBuffer = {
    synchronized {
      genericRecordWriter.setSchema(record.getSchema)
      genericRecordWriter.write(record, binaryEncoder)
      binaryEncoder.flush()

      val buffers = outputStream.getBufferList
      val result = ByteBuffer.allocate(buffers.map(_.remaining()).sum + 8)

      result.putLong(AvroSchemaStore.getFingerprint(record.getSchema))
      buffers.foreach(result.put)

      result.flip()
      result
    }
  }

  /**
    *
    * @param byteBuffer ser data.
    * @throws java.util.NoSuchElementException can't find schema.
    * @return der record.
    */
  @throws(classOf[NoSuchElementException])
  def genericRecordDer(byteBuffer: ByteBuffer): GenericData.Record = {
    synchronized {
      byteBuffer.rewind()

      val fingerprint = byteBuffer.getLong

      val schema = AvroSchemaStore.getSchemaByFingerprint(fingerprint).get
      val latestSchema = AvroSchemaStore.getLatestSchema(schema.getNamespace, schema.getName).get

      val record = new GenericData.Record(latestSchema)

      genericRecordReader.setSchema(schema)
      genericRecordReader.setExpected(latestSchema)

      inputStream.setByteBuffer(byteBuffer)
      genericRecordReader.read(record, binaryDecoder)

      record
    }
  }

  def reflectSer[T](datumWriter: ReflectDatumWriter[T], record: T): ByteBuffer = {
    synchronized {
      datumWriter.write(record, binaryEncoder)
      binaryEncoder.flush()

      val buffers = outputStream.getBufferList
      val result = ByteBuffer.allocate(buffers.map(_.remaining()).sum + 8)

      buffers.foreach(result.put)

      result.flip()
      result
    }
  }

  def reflectDer[T](datumReader: ReflectDatumReader[T], byteBuffer: ByteBuffer, reuseRecord: T): T = {
    synchronized {
      byteBuffer.rewind()

      inputStream.setByteBuffer(byteBuffer)

      datumReader.read(reuseRecord, binaryDecoder)
    }
  }
}
