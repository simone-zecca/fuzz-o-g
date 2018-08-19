package implicits

import org.apache.spark.sql.Column

object ColumnExtender {

  implicit class ColumnMethods(c: Column) {

    /**
      * adds a chain method to org.apache.spark.sql.Column
      * allowing to concatenate Column functions call:
      * col
      *   .chain(trim(_))
      *   .chain(lower(_))
      *   .chain(translate(_, 'replace', 'replacement')
      */
    def chain(t: (Column => Column)): Column = {
      t(c)
    }
  }

}
