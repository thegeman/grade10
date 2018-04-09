package science.atlarge.grade10.analysis.attribution

import science.atlarge.grade10.metrics.TimeSlicePeriodList
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.serialization.Grade10Deserializer
import science.atlarge.grade10.serialization.Grade10Serializer
import science.atlarge.grade10.util.TimeSliceId
import science.atlarge.grade10.util.TimeSliceRange

object ActivePhaseDetectionStep {

    fun execute(
            phaseMetricMappingCache: PhaseMetricMappingCache,
            resourceAttributionRules: ResourceAttributionRuleProvider
    ): ActivePhaseDetectionStepResult {
        val activePhaseTimeSlices = phaseMetricMappingCache.leafPhases
                .map { leafPhase ->
                    val leafPhaseTimeSlices = TimeSlicePeriodList(leafPhase.timeSliceRange)
                    val activeTimeSlices = phaseMetricMappingCache.leafPhaseToMetricMapping[leafPhase]!!
                            .filterIsInstance<Metric.Blocking>()
                            .filter {
                                resourceAttributionRules[leafPhase, it] == BlockingResourceAttributionRule.Full
                            }
                            .fold(leafPhaseTimeSlices) { timeSlices, metric -> timeSlices - metric.blockedTimeSlices }
                    leafPhase to activeTimeSlices
                }
                .toMap()
        return ActivePhaseDetectionStepResult(activePhaseTimeSlices)
    }

}

class ActivePhaseDetectionStepResult(
        private val activePhaseTimeSlices: Map<Phase, TimeSlicePeriodList>
) {

    fun activeIterator(
            phase: Phase,
            startTimeSlice: TimeSliceId = phase.firstTimeSlice,
            endTimeSliceInclusive: TimeSliceId = phase.lastTimeSlice
    ): PhaseActiveIterator {
        require(phase in activePhaseTimeSlices) {
            "Phase \"${phase.path}\" is not a leaf phase or not part of the analyzed execution model"
        }
        require(startTimeSlice >= phase.firstTimeSlice) {
            "Iterator cannot start before the start of the phase"
        }
        require(endTimeSliceInclusive <= phase.lastTimeSlice) {
            "Iterator cannot end past the end of the phase"
        }
        return PhaseActiveIterator(
                activePhaseTimeSlices[phase]!!,
                startTimeSlice,
                endTimeSliceInclusive
        )
    }

    fun serialize(output: Grade10Serializer) {
        // 1. Number of phases
        output.writeVarInt(activePhaseTimeSlices.size, true)
        // 2. For each phase:
        activePhaseTimeSlices.forEach { phase, timeSlices ->
            // 2.1. The phase
            output.write(phase)
            // 2.2. Number of time slice periods
            output.writeVarInt(timeSlices.periods.size, true)
            // 2.3. For each time slice period:
            timeSlices.periods.forEach { period ->
                // 2.3.1. First time slice
                output.writeLong(period.first)
                // 2.3.2. Delta from first to last time slice
                output.writeVarLong(period.last - period.first, true)
            }
        }
    }

    companion object {

        fun deserialize(input: Grade10Deserializer): ActivePhaseDetectionStepResult {
            // 1. Number of phases
            val numPhases = input.readVarInt(true)
            // 2. For each phase:
            val activePhaseTimeSlices = mutableMapOf<Phase, TimeSlicePeriodList>()
            repeat(numPhases) {
                // 2.1. The phase
                val phase = input.readPhase()!!
                // 2.2. Number of time slice periods
                val numPeriods = input.readVarInt(true)
                // 2.3. For each time slice period:
                val periodList = arrayListOf<TimeSliceRange>()
                repeat(numPeriods) {
                    // 2.3.1. First time slice
                    val first = input.readLong()
                    // 2.3.2. Delta from first to last time slice
                    val last = input.readVarLong(true) + first
                    periodList.add(first..last)
                }
                activePhaseTimeSlices[phase] = TimeSlicePeriodList(periodList)
            }
            return ActivePhaseDetectionStepResult(activePhaseTimeSlices)
        }

    }

}

class PhaseActiveIterator(
        activeTimeSlices: TimeSlicePeriodList,
        startTimeSlice: TimeSliceId,
        private val endTimeSliceInclusive: TimeSliceId
) {

    private var nextTimeSlice = startTimeSlice
    private val activePeriodIterator = activeTimeSlices.periods.iterator()
    private var currentPeriod: LongRange?
    private var inPeriod: Boolean

    init {
        var firstPeriod = if (activePeriodIterator.hasNext()) activePeriodIterator.next() else null
        while (firstPeriod != null && firstPeriod.endInclusive < startTimeSlice)
            firstPeriod = if (activePeriodIterator.hasNext()) activePeriodIterator.next() else null
        currentPeriod = firstPeriod
        inPeriod = firstPeriod != null && nextTimeSlice in firstPeriod
    }

    fun hasNext(): Boolean = nextTimeSlice <= endTimeSliceInclusive

    fun nextIsActive(): Boolean = when {
        inPeriod -> {
            nextTimeSlice++
            if (currentPeriod!!.endInclusive < nextTimeSlice) {
                currentPeriod = if (activePeriodIterator.hasNext()) activePeriodIterator.next() else null
                inPeriod = false
            }
            true
        }
        currentPeriod != null -> {
            nextTimeSlice++
            if (nextTimeSlice >= currentPeriod!!.start)
                inPeriod = true
            false
        }
        else -> {
            nextTimeSlice++
            false
        }
    }

}
