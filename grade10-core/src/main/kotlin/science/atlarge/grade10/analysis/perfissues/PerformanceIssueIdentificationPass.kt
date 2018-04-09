package science.atlarge.grade10.analysis.perfissues

import science.atlarge.grade10.analysis.PhaseHierarchyAnalysis
import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisResult
import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisRule
import science.atlarge.grade10.analysis.attribution.ResourceAttributionResult
import science.atlarge.grade10.analysis.bottlenecks.BottleneckIdentificationResult
import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.execution.PhaseType
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.model.resources.MetricType
import science.atlarge.grade10.model.resources.ResourceModel
import science.atlarge.grade10.util.FractionalTimeSliceCount

interface PerformanceIssueIdentificationPass<T> {

    val passName: String

    val description: String

    fun getRule(input: PerformanceIssueIdentificationPassInput):
            PhaseHierarchyAnalysisRule<T>

    fun extractPerformanceIssues(analysisResult: PhaseHierarchyAnalysisResult<T>): Iterable<PerformanceIssue>

    fun executePass(input: PerformanceIssueIdentificationPassInput): Iterable<PerformanceIssue> {
        return extractPerformanceIssues(PhaseHierarchyAnalysis.analyze(
                input.executionModel,
                getRule(input)
        ))
    }

}

class PerformanceIssueIdentificationPassInput(
        val executionModel: ExecutionModel,
        val resourceModel: ResourceModel,
        val resourceAttributionResult: ResourceAttributionResult,
        val bottleneckIdentificationResult: BottleneckIdentificationResult
)

interface PerformanceIssue {

    val affectedPhases: Set<Phase>?
        get() = null

    val affectedPhaseTypes: Set<PhaseType>?
        get() = null

    val affectedMetrics: Set<Metric>?
        get() = null

    val affectedMetricTypes: Set<MetricType>?
        get() = null

    val estimatedImpact: FractionalTimeSliceCount

    fun shouldDisplayAtPhase(phase: Phase): Boolean {
        val phases = affectedPhases
        if (phases != null && phase in phases) {
            return true
        }

        val phaseTypes = affectedPhaseTypes
        return phaseTypes != null && phase.type in phaseTypes
    }

    fun toDisplayString(): String

}
