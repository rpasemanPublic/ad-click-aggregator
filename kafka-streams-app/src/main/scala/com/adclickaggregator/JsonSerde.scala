package com.adclickaggregator

import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

object JsonSerde {
  private def jsonSerializer[T](implicit encoder: Encoder[T]): Serializer[T] = (_, value) =>
    value.asJson.noSpaces.getBytes("UTF-8")

  private def jsonDeserializer[T](implicit decoder: Decoder[T]): Deserializer[T] = (_, bytes) =>
    io.circe.parser.decode[T](new String(bytes, "UTF-8")) match {
      case Left(error)  => throw error
      case Right(value) => value
    }

  def apply[T](implicit encoder: Encoder[T], decoder: Decoder[T]): Serde[T] =
    Serdes.serdeFrom(jsonSerializer[T], jsonDeserializer[T])
}
