package science.atlarge.grade10.records

import science.atlarge.grade10.util.DurationNs
import science.atlarge.grade10.util.TimestampNs

/**
 * Represents an arbitrary record to be processed by perf-analyzer.
 */
interface Record

/**
 * Represents an arbitrary timestamped [Record].
 */
interface TimestampedRecord : Record {

    /**
     * Timestamp of the record in nanoseconds since the Unix epoch
     */
    val timestamp: TimestampNs

}

/**
 * Represents an arbitrary [Record] of a period with a starting time and duration.
 */
interface PeriodRecord : Record {

    /**
     * Start time of the period in nanoseconds since the Unix epoch.
     */
    val startTime: TimestampNs

    /**
     * End time of the period in nanoseconds since the Unix epoch.
     */
    val endTime: TimestampNs

    /**
     * Duration of the period in nanoseconds.
     */
    val duration: DurationNs
        get() = endTime - startTime

}

/**
 * An enumeration to describe the type of an [EventRecord].
 */
enum class EventRecordType {
    /**
     * A record indicating the start of an event.
     */
    START,
    /**
     * A record indicating the end of an event.
     */
    END,
    /**
     * A record indicating an instantaneous event.
     */
    SINGLE
}

/**
 * Record representing an event taking place in the system under test. Events can one of various types
 * (see [EventRecordType]) and any number of tags associated with it.
 *
 * @property[type] Type of the recorded event.
 * @property[tags] Tags associated with the recorded event.
 */
data class EventRecord(
        override val timestamp: TimestampNs,
        val type: EventRecordType,
        val tags: Map<Tag, String>
) : TimestampedRecord

/**
 * Represents a tag that may be associated with a value, e.g., in an [EventRecord].
 */
typealias Tag = String
