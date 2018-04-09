package science.atlarge.grade10.analysis.attribution

import science.atlarge.grade10.model.PhaseToResourceMapping
import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.model.resources.Resource
import science.atlarge.grade10.model.resources.ResourceModel
import science.atlarge.grade10.util.collectTreeNodes

class PhaseMetricMappingCache private constructor(
        val phases: List<Phase>,
        val leafPhases: List<Phase>,
        val metrics: List<Metric>,
        val consumableMetrics: List<Metric.Consumable>,
        val blockingMetrics: List<Metric.Blocking>,
        val phaseToMetricMapping: Map<Phase, List<Metric>>,
        val leafPhaseToMetricMapping: Map<Phase, List<Metric>>,
        val metricToLeafPhaseMapping: Map<Metric, List<Phase>>,
        val consumableMetricToLeafPhaseMapping: Map<Metric.Consumable, List<Phase>>,
        val blockingMetricToLeafPhaseMapping: Map<Metric.Blocking, List<Phase>>
) {

    companion object {

        fun from(
                executionModel: ExecutionModel,
                resourceModel: ResourceModel,
                phaseToResourceMapping: PhaseToResourceMapping
        ): PhaseMetricMappingCache {
            val phases = collectTreeNodes(executionModel.rootPhase) { it.subphases.values }
                    .sortedBy { it.path }
            val leafPhases = phases.filter { it.isLeaf }
            val metrics = collectTreeNodes(resourceModel.rootResource) { it.subresources.values }
                    .flatMap { it.metrics.values }
                    .sortedBy { it.path }
            val consumableMetrics = metrics.filterIsInstance<Metric.Consumable>()
            val blockingMetrics = metrics.filterIsInstance<Metric.Blocking>()

            val resourceToMetricMap = buildResourceToMetricMap(resourceModel.rootResource)

            val phaseToMetricMapping = phases.map { phase ->
                val metricSet = mutableSetOf<Metric>()
                val mapping = phaseToResourceMapping[phase]
                metricSet.addAll(mapping.metrics)
                mapping.resources.flatMapTo(metricSet) { resourceToMetricMap[it]!! }
                phase to metricSet.toList()
            }.toMap()
            val leafPhaseToMetricMapping = phaseToMetricMapping.filterKeys { it.isLeaf }

            val metricToLeafPhaseMapping = mutableMapOf<Metric, MutableList<Phase>>()
            leafPhaseToMetricMapping.forEach { leafPhase, mappedMetrics ->
                mappedMetrics.forEach { mappedMetric ->
                    metricToLeafPhaseMapping.getOrPut(mappedMetric) { arrayListOf() }
                            .add(leafPhase)
                }
            }
            val consumableMetricToLeafPhaseMapping =
                    consumableMetrics.mapNotNull { c -> metricToLeafPhaseMapping[c]?.let { c to it } }.toMap()
            val blockingMetricToLeafPhaseMapping =
                    blockingMetrics.mapNotNull { b -> metricToLeafPhaseMapping[b]?.let { b to it } }.toMap()

            return PhaseMetricMappingCache(phases, leafPhases, metrics, consumableMetrics, blockingMetrics,
                    phaseToMetricMapping, leafPhaseToMetricMapping, metricToLeafPhaseMapping,
                    consumableMetricToLeafPhaseMapping, blockingMetricToLeafPhaseMapping)
        }

        private fun buildResourceToMetricMap(rootResource: Resource): Map<Resource, List<Metric>> {
            val res = mutableMapOf<Resource, List<Metric>>()
            addResourceToMetricMap(rootResource, res)
            return res
        }

        private fun addResourceToMetricMap(
                resource: Resource,
                partialMap: MutableMap<Resource, List<Metric>>
        ): List<Metric> {
            val metrics = ArrayList(resource.metrics.values)
            resource.subresources.forEach { _, subresource ->
                metrics.addAll(addResourceToMetricMap(subresource, partialMap))
            }
            partialMap[resource] = metrics
            return metrics
        }

    }

}
