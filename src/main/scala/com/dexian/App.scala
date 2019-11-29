package com.dexian


import java.sql.Timestamp
import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.github.javafaker.Faker
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * @author ${user.name}
 */


object utils {
  def get_random_date_within(start: LocalDateTime, end: LocalDateTime): Timestamp = {
    val random = new Random()
    val duration = Duration.between(start, end).toDays.toInt
    val rand_day = random.nextInt(duration)
    Timestamp.valueOf(start.plusDays(rand_day))
  }

  case class salesRecords(date: Timestamp,
                          name: String,
                          address: String,
                          phoneNumber: String,
                          curreny: String,
                          code: String,
                          amount: Float)

  def get_one_random_record(faker: Faker, start: LocalDateTime, end: LocalDateTime): salesRecords = {
    salesRecords(
      get_random_date_within(start, end),
      faker.name().fullName(),
      faker.address().fullAddress(),
      faker.phoneNumber().phoneNumber(),
      faker.currency().code(),
      faker.code().isbn13(),
      Random.nextInt(10000000) + Random.nextFloat()
    )
  }

  def get_random_records(faker: Faker, start: LocalDateTime, end: LocalDateTime, num_of_records: Int): Seq[salesRecords] = {
    (0 to num_of_records).par.map(_ => get_one_random_record(faker, start, end)).seq
  }
}

object MainEntry extends App {
  val spark = SparkSession.builder().appName("files").master("local[*]").getOrCreate()
  import spark.implicits._

  val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
  val start_date = LocalDateTime.parse("01/03/2016 10:01:10", formatter)
  val end_date = LocalDateTime.parse("01/09/2019 00:00:00", formatter)

  val faker = new Faker()
  val records = utils.get_random_records(faker, start_date, end_date, 1e6.toInt)
  val df = spark.createDataset(records)

  df.printSchema
  df.show


  println("hello, you've succeed")
}