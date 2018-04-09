package science.atlarge.grade10.analysis

import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.Phase

object PhaseHierarchyAnalysis {

    fun <T> analyze(
            executionModel: ExecutionModel,
            analysisRule: PhaseHierarchyAnalysisRule<T>
    ): PhaseHierarchyAnalysisResult<T> {
        val results = mutableMapOf<Phase, T>()
        recurse(executionModel.rootPhase, analysisRule) { phase, result -> results[phase] = result }
        return PhaseHierarchyAnalysisResult(results)
    }

    private fun <T> recurse(
            phase: Phase,
            rule: PhaseHierarchyAnalysisRule<T>,
            addResult: (Phase, T) -> Unit
    ): T {
        val result = if (phase.isLeaf) {
            rule.analyzeLeafPhase(phase)
        } else {
            val subphaseResults = phase.subphases
                    .map { (_, subphase) ->
                        subphase to recurse(subphase, rule, addResult)
                    }
                    .toMap()
            rule.combineSubphaseResults(phase, subphaseResults)
        }

        addResult(phase, result)

        return result
    }

}

class PhaseHierarchyAnalysisResult<out T>(
        private val results: Map<Phase, T>
) {

    val phases: Set<Phase>
        get() = results.keys

    operator fun get(phase: Phase): T =
            results[phase] ?: throw IllegalArgumentException("No result found for phase \"${phase.path}\"")

}

interface PhaseHierarchyAnalysisRule<T> {

    fun analyzeLeafPhase(leafPhase: Phase): T

    fun combineSubphaseResults(compositePhase: Phase, subphaseResults: Map<Phase, T>): T

}
