package science.atlarge.grade10.util

/*
 * Type aliases to help document the time units used throughout perf-analyzer (i.e., nanoseconds and time slices)
 * without adding overhead over primitive long values.
 */

typealias TimestampNs = Long
typealias TimestampNsRange = LongRange
typealias TimestampNsArray = LongArray
typealias DurationNs = Long

typealias TimeSliceId = Long
typealias TimeSliceRange = LongRange
typealias TimeSliceCount = Long
typealias FractionalTimeSliceCount = Double

const val NANOSECONDS_PER_TIMESLICE = 1_000_000L

fun timeSliceContainingTimestamp(timestamp: TimestampNs): TimeSliceId {
    return timestamp / NANOSECONDS_PER_TIMESLICE
}

fun timeSliceForStartTimestamp(startTimestamp: TimestampNs): TimeSliceId {
    val timeSliceId = timeSliceContainingTimestamp(startTimestamp)
    return if (startTimestamp - timeSliceId * NANOSECONDS_PER_TIMESLICE >= NANOSECONDS_PER_TIMESLICE / 2) {
        timeSliceId + 1
    } else {
        timeSliceId
    }
}

fun timeSliceForEndTimestamp(endTimestamp: TimestampNs): TimeSliceId {
    val timeSliceId = timeSliceContainingTimestamp(endTimestamp)
    return if (endTimestamp - timeSliceId * NANOSECONDS_PER_TIMESLICE < NANOSECONDS_PER_TIMESLICE / 2) {
        timeSliceId - 1
    } else {
        timeSliceId
    }
}

fun startOfTimeSlice(timeSlice: TimeSliceId): TimestampNs = timeSlice * NANOSECONDS_PER_TIMESLICE
fun endOfTimeSlice(timeSlice: TimeSliceId): TimestampNs = startOfTimeSlice(timeSlice + 1) - 1
