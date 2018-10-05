package com.example.record

import org.apache.avro.reflect.{ReflectDatumReader, ReflectDatumWriter}
import org.joda.time.DateTime

trait SampleRecords[T] {
  private val datumWriter = this.createDatumWriter
  private val datumReader = this.createDatumReader
  private val reuseRecord: T = this.createReuseRecord

  def getDatumWriter: ReflectDatumWriter[T] = datumWriter
  def getDatumReader: ReflectDatumReader[T] = datumReader
  def getReuseRecord: T = reuseRecord
  def createReuseRecord: T
  def createDatumWriter: ReflectDatumWriter[T]
  def createDatumReader: ReflectDatumReader[T]
}

object SampleRecords {
  case class UserRecordv1(dateTime: String, uid: String, name: String)
  case class UserRecordv2(dateTime: String, uid: String, name: String, email: String = "null", phone: String = "null")
  case class UserRecord(dateTime: String, uid: String, name: String, email: String = "null", phone: String = "null")
  case class LoginLogRecord(dateTime: DateTime, uid: String)

  object UserRecordv1 extends SampleRecords[UserRecordv1] {
    override def createReuseRecord: UserRecordv1 = UserRecordv1("", "", "")
    override def createDatumWriter: ReflectDatumWriter[UserRecordv1] = new ReflectDatumWriter(classOf[UserRecordv1])
    override def createDatumReader: ReflectDatumReader[UserRecordv1] = new ReflectDatumReader(classOf[UserRecordv1])
  }

  object UserRecordv2 extends SampleRecords[UserRecordv2] {
    override def createReuseRecord: UserRecordv2 = UserRecordv2("", "", "")
    override def createDatumWriter: ReflectDatumWriter[UserRecordv2] = new ReflectDatumWriter(classOf[UserRecordv2])
    override def createDatumReader: ReflectDatumReader[UserRecordv2] = new ReflectDatumReader(classOf[UserRecordv2])
  }
}