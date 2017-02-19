/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.feature.tfidf

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.collection.immutable.Map

/**
 * :: Experimental ::
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 */
@Experimental
class MLHashingTF(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("hashingTF"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
    * Number of features.  Should be > 0.
    * (default = 2^18^)
    * @group param
    */
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

  setDefault(numFeatures -> (1 << 18))

  /** @group getParam */
  def getNumFeatures: Int = $(numFeatures)

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  var hashMap:Map[Any,Int] = Map.empty[Any,Int]

  def getHashMap(dataset:DataFrame):Map[Any,Int] = {
    val wordArray = dataset.select($(inputCol)).flatMap(r => r.getAs[Seq[Any]](0)).
      distinct().collect()
    wordArray.zip(0 until wordArray.length).toMap
  }

  override def transform(dataset: DataFrame): DataFrame = {
    hashMap = getHashMap(dataset)
    println("*\n"*10)
    println(hashMap.size)
    setNumFeatures(hashMap.size)
    val outputSchema = transformSchema(dataset.schema)
    val hashingTF = new MLLibHashingTF(hashMap)
    val t = udf { terms: Seq[_] => hashingTF.transform(terms) }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  override def copy(extra: ParamMap): MLHashingTF = defaultCopy(extra)
}

@Since("1.6.0")
object MLHashingTF extends DefaultParamsReadable[MLHashingTF] {

  @Since("1.6.0")
  override def load(path: String): MLHashingTF = super.load(path)
}
