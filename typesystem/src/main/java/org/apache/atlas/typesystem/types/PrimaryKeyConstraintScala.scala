package org.apache.atlas.typesystem.types

import java.util

import org.apache.commons.jexl3._
import scala.collection.JavaConversions._

case class PrimaryKeyConstraintScala(columns: java.util.List[String], isVisible: Boolean = true, displayFormat: String = null) {

  def displayValue(pkValues: java.util.Map[String, String]) : String = {

      if ( displayFormat != null ) {
        val exprString: String = displayFormat
        val jexl: JexlEngine = new JexlBuilder().create
        // Create an expression
        val jxlt: JxltEngine = jexl.createJxltEngine
        val expr: JxltEngine.Expression = jxlt.createExpression(exprString)

        // Create a context and add data
        val jc: JexlContext = new MapContext

        pkValues.map(x => jc.set(x._1, x._2))

        // Now evaluate the expression, getting the result
        expr.evaluate(jc).asInstanceOf[String]
    }

    return null;
  }
}

object PrimaryKeyConstraintScala {

  def of(columns: Array[java.lang.String]) : PrimaryKeyConstraintScala = {
    PrimaryKeyConstraintScala(columns.toList)
  }

  def of(columns: java.util.List[String]) : PrimaryKeyConstraintScala = {
    PrimaryKeyConstraintScala(columns)
  }

  def of(columns: java.util.List[String], displayFmt : String) : PrimaryKeyConstraintScala = {
    PrimaryKeyConstraintScala(columns, true, displayFmt)
  }

  def of(uniqueColumns: java.lang.Iterable[String], isVisible: Boolean, displayFormat: String) : PrimaryKeyConstraintScala = {
    PrimaryKeyConstraintScala(uniqueColumns.toList, isVisible, displayFormat)
  }
}