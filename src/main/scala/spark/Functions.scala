package spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Functions {

  import implicits.ColumnExtender._

  def clean_city(col: Column): Column = {
    col
      .chain(initcap(_))
      .chain(translate(_, "=", " "))              //text replacement: from non standard symbols to space
      .chain(translate(_, "+", " "))
      .chain(translate(_, "-", " "))
      .chain(translate(_, "_", " "))
      .chain(translate(_, ")", " "))
      .chain(translate(_, "()", " "))
      .chain(translate(_, "*", " "))
      .chain(translate(_, "&", " "))
      .chain(translate(_, "^", " "))
      .chain(translate(_, "%", " "))
      .chain(translate(_, "$", " "))
      .chain(translate(_, "#", " "))
      .chain(translate(_, "@", " "))
      .chain(translate(_, "!", " "))
      .chain(translate(_, "~", " "))
      .chain(translate(_, "`", " "))
      .chain(translate(_, "{", " "))
      .chain(translate(_, "}", " "))
      .chain(translate(_, "|", " "))
      .chain(translate(_, ":", " "))
      .chain(translate(_, ";", " "))
      .chain(translate(_, "\"", " "))
      .chain(translate(_, "'", " "))
      .chain(translate(_, "<", " "))
      .chain(translate(_, ">", " "))
      .chain(translate(_, ",", " "))
      .chain(translate(_, ".", " "))
      .chain(translate(_, "?", " "))
      .chain(translate(_, "/", " "))


      .chain(regexp_replace(_, "avenue", " "))

      .chain(regexp_replace(_, "\\s+", " "))    //spaces compression
      .chain(trim(_))
  }

}
