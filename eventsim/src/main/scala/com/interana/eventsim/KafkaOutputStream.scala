package com.interana.eventsim

import java.io.OutputStream

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer

class KafkaOutputStream(val producer: KafkaProducer[Array[Byte], Array[Byte]], val topic: String) extends OutputStream {

  val buffer = new ArrayBuffer[Byte](4096)

  override def write(i: Int) = {
    buffer.append(i.toByte)
  }

  override def flush() = {
    if (buffer.nonEmpty) {
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, buffer.toArray[Byte])
      producer.send(record)
      buffer.clear()
    }
  }

  override def close() = {
    flush()
    producer.close()
  }

}
