package science.atlarge.grade10.model.execution

import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.execution.PhaseRecordDerivationRule.Companion.TAG_PHASE
import science.atlarge.grade10.records.*
import science.atlarge.grade10.util.*

/**
 * Record representing an execution of a [Phase].
 *
 * @property[startTime] the start of this phase's execution.
 * @property[endTime] the end of this phase's execution.
 * @property[phaseType] the type of the phase that produced this record.
 * @property[phasePath] the path of the phase that produced this record. The path must be absolute and canonical.
 */
data class PhaseRecord(
        override val startTime: TimestampNs,
        override val endTime: TimestampNs,
        val phaseType: PhaseType,
        val phasePath: Path
) : PeriodRecord {

    val firstTimeSlice: TimeSliceId = timeSliceForStartTimestamp(startTime)
    val lastTimeSlice: TimeSliceId = timeSliceForEndTimestamp(endTime)
    val timeSliceDuration: TimeSliceCount
        get() = lastTimeSlice - firstTimeSlice + 1

    init {
        require(duration >= 0) { "PhaseRecord must have a non-negative duration" }
        require(phasePath.isAbsolute) { "Path of PhaseRecord must be absolute" }
        require(phasePath.isCanonical) { "Path of PhaseRecord must be canonical" }
    }

    override fun toString(): String =
            "PhaseRecord(phasePath=$phasePath, timeSlices=[$firstTimeSlice, $lastTimeSlice])"

}

/**
 * TODO: Document
 */
class PhaseRecordDerivationRule(
        val systemModel: ExecutionModelSpecification
) : TypeFilteredRecordDerivationRule<EventRecord>(EventRecord::class.java) {

    override fun matches(record: EventRecord) = TAG_PHASE in record.tags

    override fun deriveFromMatchedRecords(records: Iterable<EventRecord>) =
            records.groupBy(this::getPhaseTypeAndPathForMatched)
                    .map {
                        derivePhaseRecord(it.key.first, it.key.second, it.value)
                    }

    private fun getPhaseTypeAndPathForMatched(record: EventRecord): Pair<PhaseType, Path> {
        val phaseType = getPhaseTypeForName(record.tags[TAG_PHASE]!!, record.tags)
        return Pair(
                phaseType,
                Phase.constructPhasePath(phaseType, { repetitionKey -> record.tags[repetitionKey]!! })
        )
    }

    private fun getPhaseTypeForName(name: String, tags: Map<Tag, String>): PhaseType {
        return if (name == ROOT_NAME) {
            systemModel.rootPhaseType
        } else {
            val matchedPhaseTypes = arrayListOf<PhaseType>()
            depthFirstSearch(
                    systemModel.rootPhaseType,
                    { it.subphaseTypes.values.filter { isPhaseRepeatabilityKeySet(it, tags) } },
                    { it, _ -> if (it.name == name) matchedPhaseTypes.add(it) }
            )
            require(matchedPhaseTypes.size == 1) {
                "Did not find exactly one PhaseType called '$name' in the provided systemModel"
            }
            matchedPhaseTypes[0]
        }
    }

    private fun isPhaseRepeatabilityKeySet(phaseType: PhaseType, tags: Map<Tag, String>) =
            !phaseType.repeatability.isRepeatable || phaseType.repeatability.instanceKey in tags

    companion object {

        const val ROOT_NAME = "/"
        const val TAG_PHASE: Tag = "phase"

        private fun derivePhaseRecord(phaseType: PhaseType, phasePath: Path, records: List<Record>): PhaseRecord {
            require(records.size == 2) {
                "A PhaseRecord can only be constructed from exactly 2 records, got: $records"
            }

            val (recordA, recordB) = records
            if (recordA !is EventRecord || recordB !is EventRecord) {
                throw IllegalArgumentException("A PhaseRecord can only be constructed from EventRecords, got: $records")
            }

            val (start, end) = when {
                recordA.type == EventRecordType.START && recordB.type == EventRecordType.END -> recordA to recordB
                recordA.type == EventRecordType.END && recordB.type == EventRecordType.START -> recordB to recordA
                else -> throw IllegalArgumentException(
                        "A PhaseRecord can only be constructed from a START and END event, got: $records")
            }

            return PhaseRecord(start.timestamp, end.timestamp, phaseType, phasePath)
        }

    }

}

/**
 * TODO: Document
 */
class ParentPhaseRecordDerivationRule(
        private val parentPhaseType: PhaseType
) : TypeFilteredRecordDerivationRule<PhaseRecord>(PhaseRecord::class.java) {

    override fun matches(record: PhaseRecord) = record.phaseType.parent === parentPhaseType

    override fun deriveFromMatchedRecords(records: Iterable<PhaseRecord>): Iterable<Record> {
        val recordsByParentPath = records.groupBy { record ->
            record.phasePath.resolve("..").toCanonicalPath()
        }
        return recordsByParentPath.map { (phasePath, subPhaseRecords) ->
            val minTime = subPhaseRecords.map { it.startTime }.min()!!
            val maxTime = subPhaseRecords.map { it.endTime }.max()!!
            PhaseRecord(minTime, maxTime, parentPhaseType, phasePath)
        }
    }

}

/**
 * TODO: Document
 */
class PhaseRenameRecordModificationRule(
        private val fromName: String,
        private val toName: String
) : RecordModificationRule {

    override fun modify(recordStore: RecordStore) {
        val newRecords = arrayListOf<EventRecord>()
        recordStore.removeIf {
            if (it is EventRecord && it.tags[TAG_PHASE] == fromName) {
                val newRecord = EventRecord(it.timestamp, it.type, it.tags + (TAG_PHASE to toName))
                newRecords.add(newRecord)
                true
            } else {
                false
            }
        }
        recordStore.addAll(newRecords)
    }

}
