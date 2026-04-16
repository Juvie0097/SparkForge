package com.sparkforge.advanced

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}

/**
 * Toy Catalyst optimizer rule: rewrite `cast(amount as int) > <lit>` into
 * `amount > <lit>::double` so that the predicate can be pushed down to
 * Parquet (Cast on the left side blocks pushdown).
 *
 * This is the kind of thing you'd write when:
 *   - users keep submitting SQL that wraps casts on the wrong side,
 *   - you want to enforce policies (e.g. forbid certain UDFs),
 *   - you want to add custom join hints to internal SQL dialects.
 *
 * To register on session creation:
 *   spark = SparkSession.builder()
 *     .withExtensions(new MyExtensions)
 *     .getOrCreate()
 */
class StripUnnecessaryNumericCast extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(cond, _) =>
      val rewritten = cond.transform {
        // Use guards instead of unapply to stay portable across Spark
        // versions where Cast's parameter list changes.
        case GreaterThan(c: Cast, Literal(v, _))
            if (c.dataType == IntegerType || c.dataType == LongType)
               && c.child.dataType == DoubleType =>
          val asDouble = v match {
            case i: java.lang.Integer => i.doubleValue()
            case l: java.lang.Long    => l.doubleValue()
            case other                => other.toString.toDouble
          }
          GreaterThan(c.child, Literal(asDouble))
      }
      if (rewritten fastEquals cond) f else f.copy(condition = rewritten.asInstanceOf[Expression])
  }
}

class MyExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectOptimizerRule(_ => new StripUnnecessaryNumericCast)
  }
}
