package spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Functions {

  //adding chain method to spark.sql.Column
  import implicits.ColumnExtender._

  val REPLACEMENT: String = " "

  def clean_city(col: Column): Column = {
    col
      .chain(initcap(_))
      .chain(translate(_, "=", REPLACEMENT))              //text replacement: from non standard symbols to space
      .chain(translate(_, "+", REPLACEMENT))
      .chain(translate(_, "-", REPLACEMENT))
      .chain(translate(_, "_", REPLACEMENT))
      .chain(translate(_, ")", REPLACEMENT))
      .chain(translate(_, "()", REPLACEMENT))
      .chain(translate(_, "*", REPLACEMENT))
      .chain(translate(_, "&", REPLACEMENT))
      .chain(translate(_, "^", REPLACEMENT))
      .chain(translate(_, "%", REPLACEMENT))
      .chain(translate(_, "$", REPLACEMENT))
      .chain(translate(_, "#", REPLACEMENT))
      .chain(translate(_, "@", REPLACEMENT))
      .chain(translate(_, "!", REPLACEMENT))
      .chain(translate(_, "~", REPLACEMENT))
      .chain(translate(_, "`", REPLACEMENT))
      .chain(translate(_, "{", REPLACEMENT))
      .chain(translate(_, "}", REPLACEMENT))
      .chain(translate(_, "|", REPLACEMENT))
      .chain(translate(_, ":", REPLACEMENT))
      .chain(translate(_, ";", REPLACEMENT))
      .chain(translate(_, "\"", REPLACEMENT))
      .chain(translate(_, "\\", REPLACEMENT))
      .chain(translate(_, "'", REPLACEMENT))
      .chain(translate(_, "<", REPLACEMENT))
      .chain(translate(_, ">", REPLACEMENT))
      .chain(translate(_, ",", REPLACEMENT))
      .chain(translate(_, ".", REPLACEMENT))
      .chain(translate(_, "?", REPLACEMENT))
      .chain(translate(_, "/", REPLACEMENT))


      .chain(regexp_replace(_, "avenue", REPLACEMENT))

      .chain(regexp_replace(_, "\\s+", REPLACEMENT))    //spaces compression
      .chain(trim(_))
  }

}
