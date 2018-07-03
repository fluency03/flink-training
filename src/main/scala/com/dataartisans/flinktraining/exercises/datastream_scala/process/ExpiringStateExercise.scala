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

package com.dataartisans.flinktraining.exercises.datastream_scala.process

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{TaxiFare, TaxiRide}
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{CheckpointedTaxiFareSource, CheckpointedTaxiRideSource}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{ExerciseBase, MissingSolutionException}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
  * The "Expiring State" exercise of the Flink training
  * (http://training.data-artisans.com).
  *
  * The goal for this exercise is to enrich TaxiRides with fare information.
  *
  * Parameters:
  * -rides path-to-input-file
  * -fares path-to-input-file
  *
  */
object ExpiringStateExercise {
  val unmatchedRides: OutputTag[TaxiRide] = new OutputTag[TaxiRide]("unmatchedRides") {}
  val unmatchedFares: OutputTag[TaxiFare] = new OutputTag[TaxiFare]("unmatchedFares") {}

  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val ridesFile = params.get("rides", ExerciseBase.pathToRideData)
    val faresFile = params.get("fares", ExerciseBase.pathToFareData)

    val servingSpeedFactor = 600 // 10 minutes worth of events are served every second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env
      .addSource(rideSourceOrTest(new CheckpointedTaxiRideSource(ridesFile, servingSpeedFactor)))
      .filter { ride => ride.isStart && (ride.rideId % 1000 != 0) }
      .keyBy("rideId")

    val fares = env
      .addSource(fareSourceOrTest(new CheckpointedTaxiFareSource(faresFile, servingSpeedFactor)))
      .keyBy("rideId")

    val processed = rides.connect(fares).process(new EnrichmentFunction)

    printOrTest(processed.getSideOutput[TaxiFare](unmatchedFares))

    env.execute("ExpiringState (scala)")
  }

  case class CountWithTimestamp(count: Long, lastModified: Long)

  class EnrichmentFunction extends CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

    val fareDescriptor = new ValueStateDescriptor[TaxiFare]("TaxiFare", createTypeInformation[TaxiFare])
    val rideDescriptor = new ValueStateDescriptor[TaxiRide]("TaxiRide", createTypeInformation[TaxiRide])

    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(rideDescriptor)
    lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(fareDescriptor)

    override def processElement1(ride: TaxiRide,
                                 context: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      fareState.value match {
        case null =>
          rideState.update(ride)
          context.timerService.registerEventTimeTimer(ride.getEventTime)
        case f =>
          fareState.clear()
          out.collect(ride, f)
      }
    }

    override def processElement2(fare: TaxiFare,
                                 context: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context,
                                 out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      rideState.value match {
        case null =>
          fareState.update(fare)
          context.timerService.registerEventTimeTimer(fare.getEventTime)
        case r =>
          rideState.clear()
          out.collect(r, fare)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#OnTimerContext,
                         out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (fareState.value != null) {
        ctx.output(unmatchedFares, fareState.value)
        fareState.clear()
      }
      if (rideState.value != null) {
        ctx.output(unmatchedRides, rideState.value)
        rideState.clear()
      }
    }

  }

}
