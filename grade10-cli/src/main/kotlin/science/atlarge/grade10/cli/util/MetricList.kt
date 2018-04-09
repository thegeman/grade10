package science.atlarge.grade10.cli.util

import science.atlarge.grade10.analysis.attribution.ResourceAttributionResult
import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.model.resources.MetricClass
import science.atlarge.grade10.model.resources.MetricType
import science.atlarge.grade10.model.resources.ResourceModel
import science.atlarge.grade10.util.collectTreeNodes
import java.nio.file.Path as JPath

class MetricList(
        private val metrics: Set<Metric>
) {

    private val pathToIdentifierMap = mutableMapOf<Path, String>()
    private val identifierToPathMap = mutableMapOf<String, Path>()

    init {
        fun add(path: Path, identifier: String) {
            pathToIdentifierMap[path] = identifier
            identifierToPathMap[identifier] = path
        }

        val (blockingMetrics, consumableMetrics) = metrics.partition { it.type.metricClass == MetricClass.BLOCKING }
        val maxMetrics = maxOf(blockingMetrics.size, consumableMetrics.size)
        val maxMetricsPerType = metrics.groupBy { it.type }.map { it.value.size }.max() ?: 0

        blockingMetrics.groupBy({ it.type }, { it.path })
                .map { (metricType, metricPaths) -> metricType.path to metricPaths.sorted() }
                .sortedBy { it.first }
                .forEachIndexed { i, (metricTypePath, metricPaths) ->
                    val metricTypeId = "b${padInt(i + 1, maxMetrics)}"
                    add(metricTypePath, metricTypeId)
                    metricPaths.forEachIndexed { j, metricPath ->
                        if (metricPath != metricTypePath) {
                            add(metricPath, "$metricTypeId-${padInt(j + 1, maxMetricsPerType)}")
                        }
                    }
                }
        consumableMetrics.groupBy({ it.type }, { it.path })
                .map { (metricType, metricPaths) -> metricType.path to metricPaths.sorted() }
                .sortedBy { it.first }
                .forEachIndexed { i, (metricTypePath, metricPaths) ->
                    val metricTypeId = "c${padInt(i + 1, maxMetrics)}"
                    add(metricTypePath, metricTypeId)
                    metricPaths.forEachIndexed { j, metricPath ->
                        if (metricPath != metricTypePath) {
                            add(metricPath, "$metricTypeId-${padInt(j + 1, maxMetricsPerType)}")
                        }
                    }
                }
    }

    fun metricToIdentifier(metric: Metric): String = pathToIdentifier(metric.path)

    fun metricTypeToIdentifier(metricType: MetricType): String = pathToIdentifier(metricType.path)

    fun pathToIdentifier(path: Path): String = pathToIdentifierMap[path]
            ?: throw IllegalArgumentException("No identifier found for path \"$path\"")

    fun identifierToPath(identifier: String): Path = identifierToPathMap[identifier]
            ?: throw IllegalArgumentException("No path found for identifier \"$identifier\"")

    fun writeToFile(outputDirectory: JPath, overwriteIfExists: Boolean = false) {
        writeSelectedMetricsToFile(outputDirectory, metrics, overwriteIfExists)
    }

    fun writeSelectedMetricsToFile(
            outputDirectory: JPath,
            phase: Phase,
            resourceAttributionResult: ResourceAttributionResult,
            overwriteIfExists: Boolean = false
    ) {
        writeSelectedMetricsToFile(
                outputDirectory,
                metricsUsedByPhase(phase, resourceAttributionResult),
                overwriteIfExists
        )
    }

    fun writeSelectedMetricsToFile(
            outputDirectory: JPath,
            metrics: Iterable<Metric>,
            overwriteIfExists: Boolean = false
    ) {
        if (!overwriteIfExists && outputDirectory.resolve(METRIC_LIST_FILENAME).toFile().exists()) {
            return
        }

        val sortedIdentifiers = metrics
                .flatMap { listOf(it.path, it.type.path) }
                .map { pathToIdentifier(it) }
                .toSet()
                .sorted()

        outputDirectory.resolve(METRIC_LIST_FILENAME).toFile().bufferedWriter().use { writer ->
            writer.appendln("id\tpath\ttype")
            sortedIdentifiers.forEach { id ->
                val path = identifierToPath(id)

                writer.apply {
                    append(id)
                    append('\t')
                    append(path.toString())
                    append('\t')
                    appendln(if (id.startsWith('b')) "blocking" else "consumable")
                }
            }
        }
    }

    companion object {

        const val METRIC_LIST_FILENAME = "metric-list.tsv"

        fun fromResourceModel(resourceModel: ResourceModel): MetricList {
            val metrics = collectTreeNodes(resourceModel.rootResource) { it.subresources.values }
                    .flatMap { it.metrics.values }
                    .toSet()
            return MetricList(metrics)
        }

        private fun padInt(value: Int, maxValue: Int): String {
            require(value >= 0 && maxValue >= 0)
            return when {
                maxValue >= 1_000_000_000 -> String.format("%010d", value)
                maxValue >= 100_000_000 -> String.format("%09d", value)
                maxValue >= 10_000_000 -> String.format("%08d", value)
                maxValue >= 1_000_000 -> String.format("%07d", value)
                maxValue >= 100_000 -> String.format("%06d", value)
                maxValue >= 10_000 -> String.format("%05d", value)
                maxValue >= 1_000 -> String.format("%04d", value)
                maxValue >= 100 -> String.format("%03d", value)
                maxValue >= 10 -> String.format("%02d", value)
                else -> value.toString()
            }
        }

        // TODO: Replace with composite resource attribution result when available
        fun metricsUsedByPhase(phase: Phase, resourceAttributionResult: ResourceAttributionResult): Set<Metric> {
            return if (phase.isComposite) {
                val subphases = collectTreeNodes(phase) { it.subphases.values }
                subphases.flatMap { subphase ->
                    if (subphase in resourceAttributionResult.resourceAttribution.phases) {
                        resourceAttributionResult.resourceAttribution[subphase].metrics
                    } else {
                        emptyList<Metric>()
                    }
                }.toSet()
            } else {
                resourceAttributionResult.resourceAttribution[phase].metrics
            }
        }

    }

}
