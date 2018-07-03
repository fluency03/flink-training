/*
 * Copyright 2017 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_scala.state

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{TaxiFare, TaxiRide}
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{TaxiFareSource, TaxiRideSource}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{ExerciseBase, MissingSolutionException}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * The "Stateful Enrichment" exercise of the Flink training
  * (http://training.data-artisans.com).
  *
  * The goal for this exercise is to enrich TaxiRides with fare information.
  *
  * Parameters:
  * -rides path-to-input-file
  * -fares path-to-input-file
  *
  */
object RidesAndFaresExercise {
  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val ridesFile = params.get("rides", ExerciseBase.pathToRideData)
    val faresFile = params.get("fares", ExerciseBase.pathToFareData)

    val delay = 60;               // at most 60 seconds of delay
    val servingSpeedFactor = 1800 // 30 minutes worth of events are served every second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env
      .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
      .filter { ride => ride.isStart }
      .keyBy("rideId")

    val fares = env
      .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor)))
      .keyBy("rideId")

    val processed = rides
      .connect(fares)
      .flatMap(new EnrichmentFunction2)

    printOrTest(processed)

    env.execute("Join Rides with Fares (scala RichCoFlatMap)")
  }

  // ID-2-Fare
//  class EnrichmentFunction extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {
//
//    var fares: MapState[Long, TaxiFare] = _
//    val mapDescriptor = new MapStateDescriptor[Long, TaxiFare](
//      "id-to-fare",
//      createTypeInformation[Long],
//      createTypeInformation[TaxiFare])
//
//    override def open(parameters: Configuration): Unit = {
//      fares = getRuntimeContext.getMapState(mapDescriptor)
//    }
//
//    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
//      out.collect(ride, fares.get(ride.rideId))
//    }
//
//    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
//      fares.put(fare.rideId, fare)
//    }
//
//  }

  class EnrichmentFunction2 extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

    var fareState: ValueState[TaxiFare] = _
    var rideState: ValueState[TaxiRide] = _
    val fareDescriptor = new ValueStateDescriptor[TaxiFare]("TaxiFare", createTypeInformation[TaxiFare])
    val rideDescriptor = new ValueStateDescriptor[TaxiRide]("TaxiRide", createTypeInformation[TaxiRide])

    override def open(parameters: Configuration): Unit = {
      fareState = getRuntimeContext.getState(fareDescriptor)
      rideState = getRuntimeContext.getState(rideDescriptor)
    }

    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (fareState.value() == null) rideState.update(ride)
      else out.collect(ride, fareState.value())
    }

    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (rideState.value() == null) fareState.update(fare)
      else out.collect((rideState.value(), fare))
    }

  }

//  class EnrichmentFunction3 extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {
//
//    var fareState: ValueState[TaxiFare] = _
//    val fareDescriptor = new ValueStateDescriptor[TaxiFare]("TaxiFare", createTypeInformation[TaxiFare])
//
//    override def open(parameters: Configuration): Unit = {
//      fareState = getRuntimeContext.getState(fareDescriptor)
//    }
//
//    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
//      out.collect(ride, fareState.value())
//    }
//
//    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
//      if (fareState.value() == null) fareState.update(fare)
//    }
//
//  }

}
