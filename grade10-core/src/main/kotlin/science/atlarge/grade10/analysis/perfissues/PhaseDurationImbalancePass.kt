package science.atlarge.grade10.analysis.perfissues

import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisResult
import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisRule
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.execution.PhaseType
import science.atlarge.grade10.model.execution.PhaseTypeRepeatability
import science.atlarge.grade10.util.FractionalTimeSliceCount
import science.atlarge.grade10.util.TimeSliceCount

object PhaseDurationImbalancePass : PerformanceIssueIdentificationPass<PhaseDurationImbalancePhaseResult> {

    override val passName: String
        get() = "Phase Duration Imbalance"
    override val description: String
        get() = "Phase Duration Imbalance"

    override fun getRule(input: PerformanceIssueIdentificationPassInput):
            PhaseHierarchyAnalysisRule<PhaseDurationImbalancePhaseResult> {
        return PhaseDurationImbalanceAnalysisRule
    }

    override fun extractPerformanceIssues(
            analysisResult: PhaseHierarchyAnalysisResult<PhaseDurationImbalancePhaseResult>
    ): Iterable<PerformanceIssue> {
        return analysisResult.phases.flatMap { phase ->
            val phaseResults = analysisResult[phase]
            phaseResults.imbalanceStatisticsPerPhaseType.map { (targetPhaseType, stats) ->
                PhaseImbalancePerformanceIssue(phase, targetPhaseType, stats)
            }
        }
    }

}

private object PhaseDurationImbalanceAnalysisRule : PhaseHierarchyAnalysisRule<PhaseDurationImbalancePhaseResult> {

    override fun analyzeLeafPhase(leafPhase: Phase): PhaseDurationImbalancePhaseResult {
        return PhaseDurationImbalancePhaseResult(mapOf(
                leafPhase.type to PhaseDurationImbalanceStatistics(leafPhase.timeSliceDuration)
        ))
    }

    override fun combineSubphaseResults(
            compositePhase: Phase,
            subphaseResults: Map<Phase, PhaseDurationImbalancePhaseResult>
    ): PhaseDurationImbalancePhaseResult {
        val results = hashMapOf<PhaseType, PhaseDurationImbalanceStatistics>()
        subphaseResults.entries.groupBy({ it.key.type }, { it.value.imbalanceStatisticsPerPhaseType })
                .forEach { subphaseType, subphaseTypeResults ->
                    if (subphaseTypeResults.size == 1) {
                        results.putAll(subphaseTypeResults.first())
                    } else {
                        subphaseTypeResults.flatMap { it.toList() }
                                .groupBy({ it.first }, { it.second })
                                .forEach { phaseType, imbalanceStatistics ->
                                    results[phaseType] = combinePhaseTypeImbalanceStatistics(imbalanceStatistics,
                                            subphaseType.repeatability)
                                }
                    }
                }
        results[compositePhase.type] = PhaseDurationImbalanceStatistics(compositePhase.timeSliceDuration)
        return PhaseDurationImbalancePhaseResult(results)
    }

    private fun combinePhaseTypeImbalanceStatistics(
            imbalanceStatistics: List<PhaseDurationImbalanceStatistics>,
            phaseTypeRepeatability: PhaseTypeRepeatability
    ): PhaseDurationImbalanceStatistics {
        val isAggregatePhaseTypeSequential = when (phaseTypeRepeatability) {
            is PhaseTypeRepeatability.SequentialRepeated -> true
            is PhaseTypeRepeatability.ConcurrentRepeated -> false
            else -> throw IllegalArgumentException()
        }
        val newCount = imbalanceStatistics.map { it.phaseCount }.sum()
        val newMin = imbalanceStatistics.map { it.minPhaseDuration }.min()!!
        val newMax = imbalanceStatistics.map { it.maxPhaseDuration }.max()!!
        val newTotal = imbalanceStatistics.map { it.totalPhaseDuration }.sum()
        val newIdeal: FractionalTimeSliceCount
        val newActual: TimeSliceCount
        if (isAggregatePhaseTypeSequential) {
            newIdeal = imbalanceStatistics.map { it.idealSequentialTime }.sum()
            newActual = imbalanceStatistics.map { it.actualSequentialTime }.sum()
        } else {
            newIdeal = imbalanceStatistics.map { it.idealSequentialTime }.sum() / imbalanceStatistics.size
            newActual = imbalanceStatistics.map { it.actualSequentialTime }.max()!!
        }
        return PhaseDurationImbalanceStatistics(newCount, newMin, newMax, newTotal, newIdeal, newActual)
    }

}

class PhaseDurationImbalancePhaseResult(
        val imbalanceStatisticsPerPhaseType: Map<PhaseType, PhaseDurationImbalanceStatistics>
) {

    val phaseTypes = imbalanceStatisticsPerPhaseType.keys

    operator fun get(phaseType: PhaseType): PhaseDurationImbalanceStatistics {
        return imbalanceStatisticsPerPhaseType[phaseType]
                ?: throw IllegalArgumentException("No result found for phase type \"${phaseType.path}\"")
    }

}

data class PhaseDurationImbalanceStatistics(
        val phaseCount: Int,
        val minPhaseDuration: TimeSliceCount,
        val maxPhaseDuration: TimeSliceCount,
        val totalPhaseDuration: TimeSliceCount,
        val idealSequentialTime: FractionalTimeSliceCount,
        val actualSequentialTime: TimeSliceCount
) {

    constructor(phaseDuration: TimeSliceCount) : this(1, phaseDuration, phaseDuration, phaseDuration,
            phaseDuration.toDouble(), phaseDuration)

}

class PhaseImbalancePerformanceIssue(
        val aggregatePhase: Phase,
        val targetPhaseType: PhaseType,
        val imbalanceStatistics: PhaseDurationImbalanceStatistics
) : PerformanceIssue {

    override val affectedPhases: Set<Phase> = setOf(aggregatePhase)
    override val affectedPhaseTypes: Set<PhaseType> = setOf(targetPhaseType)

    override val estimatedImpact: FractionalTimeSliceCount
        get() = imbalanceStatistics.actualSequentialTime - imbalanceStatistics.idealSequentialTime

    override fun toDisplayString(): String {
        return "Imbalance in phases of type \"${targetPhaseType.path}\" on phase \"${aggregatePhase.path}\""
    }

}
