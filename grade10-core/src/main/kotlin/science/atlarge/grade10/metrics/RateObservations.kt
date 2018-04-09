package science.atlarge.grade10.metrics

import science.atlarge.grade10.util.*

abstract class RateObservations(
        val earliestTime: TimestampNs,
        val latestTime: TimestampNs,
        val numObservations: Int
) {

    val firstTimeSlice: TimeSliceId = timeSliceForStartTimestamp(earliestTime)
    val lastTimeSlice: TimeSliceId = timeSliceForEndTimestamp(latestTime)

    abstract fun observationIterator(): RateObservationPeriodIterator

    abstract fun observationIteratorForTimeSlices(
            startTimeSlice: TimeSliceId = firstTimeSlice,
            endTimeSliceInclusive: TimeSliceId = lastTimeSlice
    ): RateObservationPeriodIterator

    companion object {

        fun from(timestamps: TimestampNsArray, values: DoubleArray): RateObservations {
            return RateObservationsImpl.from(timestamps, values)
        }

    }

}

interface RateObservationPeriodIterator {

    val periodStartTimestamp: TimestampNs
    val periodEndTimestamp: TimestampNs

    val periodStartTimeSlice: TimeSliceId
    val periodEndTimeSlice: TimeSliceId
    val periodTimeSliceCount: TimeSliceCount
        get() = periodEndTimeSlice - periodStartTimeSlice + 1

    val observation: Double

    fun hasNext(): Boolean

    fun nextObservationPeriod()

}

class RateObservationsImpl private constructor(
        private val timestamps: TimestampNsArray,
        private val observations: DoubleArray
) : RateObservations(timestamps.first() + 1, timestamps.last(), observations.size) {

    override fun observationIterator(): RateObservationPeriodIterator {
        return Iterator(0, numObservations - 1)
    }

    override fun observationIteratorForTimeSlices(
            startTimeSlice: TimeSliceId,
            endTimeSliceInclusive: TimeSliceId
    ): RateObservationPeriodIterator {
        require(startTimeSlice >= firstTimeSlice) { "Iterator cannot start before the first observation" }
        require(endTimeSliceInclusive <= endTimeSliceInclusive) {
            "Iterator cannot end after the last observation"
        }

        if (startTimeSlice > endTimeSliceInclusive) {
            return Iterator(0, -1)
        }

        // TODO: Compute periods for start and end time slice more efficiently using binary search
        var firstPeriod = 0
        while (timeSliceForStartTimestamp(timestamps[firstPeriod + 1] + 1) <= startTimeSlice) {
            firstPeriod++
        }
        var endPeriod = numObservations - 1
        while (timeSliceForEndTimestamp(timestamps[endPeriod]) >= endTimeSliceInclusive) {
            endPeriod--
        }

        return Iterator(firstPeriod, endPeriod)
    }

    private fun validate() {
        for (i in 0 until timestamps.size - 1) {
            require(timestamps[i] <= timestamps[i + 1]) { "Timestamps must be in non-decreasing order" }

            val periodStartTimeSlice = timeSliceForStartTimestamp(timestamps[i] + 1)
            val periodEndTimeSlice = timeSliceForEndTimestamp(timestamps[i + 1])
            require(periodStartTimeSlice <= periodEndTimeSlice) {
                "Observation periods must be at least one time slice long"
            }
        }
    }

    private inner class Iterator(firstPeriodIndex: Int, val lastPeriodIndex: Int) : RateObservationPeriodIterator {

        override var periodStartTimestamp: TimestampNs = 0L
        override var periodEndTimestamp: TimestampNs = -1L
        override var periodStartTimeSlice: TimeSliceId = 0L
        override var periodEndTimeSlice: TimeSliceId = -1L
        override var observation: Double = 0.0

        var nextPeriodIndex = firstPeriodIndex

        override fun hasNext() = nextPeriodIndex <= lastPeriodIndex

        override fun nextObservationPeriod() {
            periodStartTimestamp = timestamps[nextPeriodIndex] + 1
            periodEndTimestamp = timestamps[nextPeriodIndex + 1]
            periodStartTimeSlice = timeSliceForStartTimestamp(periodStartTimestamp)
            periodEndTimeSlice = timeSliceForEndTimestamp(periodEndTimestamp)
            observation = observations[nextPeriodIndex]
            nextPeriodIndex++
        }

    }

    companion object {

        fun from(timestamps: TimestampNsArray, observations: DoubleArray): RateObservationsImpl {
            return if (timestamps.isEmpty() && observations.isEmpty()) {
                RateObservationsImpl(longArrayOf(0L), doubleArrayOf())
            } else {
                require(timestamps.size == observations.size + 1) {
                    "There must be an equal number of observations and observation periods"
                }
                RateObservationsImpl(timestamps, observations).also { it.validate() }
            }
        }

    }

}
