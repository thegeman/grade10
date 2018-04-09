package science.atlarge.grade10.model.execution

import science.atlarge.grade10.model.Path
import science.atlarge.grade10.records.Record
import science.atlarge.grade10.util.depthFirstSearch

/**
 * TODO: Document
 */
fun jobExecutionModelFromRecords(
        records: Iterable<Record>,
        executionModelSpecification: ExecutionModelSpecification
): ExecutionModel {
    val phaseRecordsByType = records.filterIsInstance<PhaseRecord>().groupBy(PhaseRecord::phaseType)

    // Sanity check
    phaseRecordsByType.keys.forEach {
        require(it.root === executionModelSpecification.rootPhaseType) {
            "All PhaseRecords must correspond to the same execution model specification"
        }
    }

    val rootPhaseRecords = phaseRecordsByType[executionModelSpecification.rootPhaseType] ?: emptyList()
    require(rootPhaseRecords.size == 1) {
        "Exactly one record for the root phase is required, found ${rootPhaseRecords.size}"
    }
    val rootPhase = Phase(executionModelSpecification.rootPhaseType, null, rootPhaseRecords[0])

    val createdPhases: MutableMap<Path, Phase> = mutableMapOf(rootPhase.path to rootPhase)
    depthFirstSearch(executionModelSpecification.rootPhaseType, { it.subphaseTypes.values }) { type, _ ->
        if (!type.isRoot) {
            val recordsForType = phaseRecordsByType[type] ?: emptyList()
            recordsForType.forEach { record ->
                val parentPath = record.phasePath.resolve("..").toCanonicalPath()
                val parentPhase = createdPhases[parentPath] ?: throw IllegalArgumentException(
                        "Missing phase at path \"$parentPath\" required by child phase at path \"${record.phasePath}\""
                )
                val repetitionId = if (record.phaseType.repeatability.isRepeatable) {
                    extractInstanceId(record.phasePath.pathComponents.last())
                } else {
                    ""
                }
                createdPhases[record.phasePath] = Phase(record.phaseType, parentPhase, record, repetitionId)
            }
        }
    }

    return ExecutionModel(executionModelSpecification, rootPhase)
}

private val INSTANCE_ID_REGEX = Regex(""".*\[([^=]*=)?([^=]*)]""")

private fun extractInstanceId(phaseName: String): String {
    return INSTANCE_ID_REGEX.find(phaseName)?.groupValues?.get(2) ?: throw IllegalArgumentException(
            "Given phase name is invalid or contains no instance ID: \"$phaseName\""
    )
}
