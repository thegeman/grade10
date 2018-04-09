package science.atlarge.grade10.monitor.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import science.atlarge.grade10.metrics.RateObservations

class DoubleRateMetricSerializer : Serializer<RateObservations>(true, true) {

    override fun write(kryo: Kryo, output: Output, rateObservations: RateObservations) {
        rateObservations.apply {
            // 1. Number of observations
            output.writeVarInt(rateObservations.numObservations, true)
            // 2. End of 0'th period
            var lastTimestamp = rateObservations.earliestTime - 1
            output.writeLong(lastTimestamp)
            // 3. For each observation:
            val iter = rateObservations.observationIterator()
            while (iter.hasNext()) {
                iter.nextObservationPeriod()
                // 3.1. Delta with the last timestamp
                output.writeVarLong(iter.periodEndTimestamp - lastTimestamp, true)
                lastTimestamp = iter.periodEndTimestamp
                // 3.2. Observation value
                output.writeDouble(iter.observation)
            }
        }
    }

    override fun read(kryo: Kryo, input: Input, type: Class<RateObservations>): RateObservations {
        // 1. Number of observations
        val numObservations = input.readVarInt(true)
        val timestamps = LongArray(numObservations + 1)
        val observations = DoubleArray(numObservations)
        // 2. End of 0'th period
        timestamps[0] = input.readLong()
        // 3. For each observation:
        repeat(numObservations) { i ->
            // 3.1. Delta with the last timestamp
            val delta = input.readVarLong(true)
            timestamps[i + 1] = timestamps[i] + delta
            // 3.2. Observation value
            observations[i] = input.readDouble()
        }
        return RateObservations.from(timestamps, observations)
    }

}
