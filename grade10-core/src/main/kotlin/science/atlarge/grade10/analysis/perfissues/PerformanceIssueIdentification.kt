package science.atlarge.grade10.analysis.perfissues

import science.atlarge.grade10.analysis.attribution.ResourceAttributionResult
import science.atlarge.grade10.analysis.bottlenecks.BottleneckIdentificationResult
import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.ResourceModel

object PerformanceIssueIdentification {

    fun execute(
            executionModel: ExecutionModel,
            resourceModel: ResourceModel,
            performanceIssueIdentificationSettings: PerformanceIssueIdentificationSettings,
            resourceAttributionResult: ResourceAttributionResult,
            bottleneckIdentificationResult: BottleneckIdentificationResult
    ): PerformanceIssueIdentificationResult {
        val input = PerformanceIssueIdentificationPassInput(executionModel, resourceModel, resourceAttributionResult,
                bottleneckIdentificationResult)
        return PerformanceIssueIdentificationResult(
                performanceIssueIdentificationSettings.issueIdentificationPasses.map {
                    it.passName to it.executePass(input).toList()
                }.toMap()
        )
    }

}


class PerformanceIssueIdentificationSettings(
        val issueIdentificationPasses: List<PerformanceIssueIdentificationPass<*>>
)

class PerformanceIssueIdentificationResult(
        private val issuesByPass: Map<String, List<PerformanceIssue>>
) {

    val issueIdentificationPasses: Set<String>
        get() = issuesByPass.keys

    val results: Sequence<PerformanceIssue>
        get() = issuesByPass.values.asSequence().flatten()

    operator fun get(passName: String): List<PerformanceIssue> = issuesByPass[passName]
            ?: throw IllegalArgumentException("No result found for pass \"$passName\"")

    fun resultsDisplayedAt(phase: Phase): Sequence<PerformanceIssue> {
        return results.filter { it.shouldDisplayAtPhase(phase) }
    }

}
