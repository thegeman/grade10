package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.cli.Command
import science.atlarge.grade10.cli.util.MetricList
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import java.nio.file.Path

object ExportResourceAttributionCommand : Command {

    const val CONSUMABLE_RESOURCE_ATTRIBUTION_FILENAME = "consumable-resource-attribution.tsv"

    override val name: String
        get() = "export-resource-attribution"
    override val shortHelpMessage: String
        get() = ""
    override val longHelpMessage: String
        get() = ""

    private val usage = "Correct usage: ${name} [--recursive] [--force] [--exclude-composite] [path-to-phase]"

    override fun process(arguments: List<String>, cliState: CliState) {
        var recursive = false
        var force = false
        var excludeComposite = false
        var path: String? = null
        arguments.forEach {
            when (it) {
                "--recursive" -> recursive = true
                "--force" -> force = true
                "--exclude-composite" -> excludeComposite = true
                else -> {
                    if (path == null) path = it
                    else {
                        println("Unexpected argument: $it")
                        println(usage)
                        return
                    }
                }
            }
        }

        val phase = cliState.resolvePhaseOrPrintError(path ?: ".") ?: return

        if (recursive) {
            exportRecursive(phase, force, excludeComposite, cliState)
        } else {
            exportForPhase(phase, force, excludeComposite, cliState)
        }
    }

    private fun exportRecursive(
            phase: Phase,
            force: Boolean,
            excludeComposite: Boolean,
            cliState: CliState
    ) {
        exportForPhase(phase, force, excludeComposite, cliState)
        phase.subphases.forEach { _, subphase ->
            exportRecursive(subphase, force, excludeComposite, cliState)
        }
    }

    private fun exportForPhase(
            phase: Phase,
            force: Boolean,
            excludeComposite: Boolean,
            cliState: CliState
    ) {
        if (phase.isComposite && excludeComposite) {
            return
        }

        val phaseOutputPath = cliState.phaseOutputPath(phase)
        if (!phaseOutputPath.toFile().exists()) {
            phaseOutputPath.toFile().mkdirs()
        }

        cliState.metricList.writeSelectedMetricsToFile(phaseOutputPath, phase,
                cliState.grade10JobResult.resourceAttributionResult, force)

        writeMetricData(phaseOutputPath, phase, force, cliState)
    }

    private fun writeMetricData(phaseOutputPath: Path, phase: Phase, overwriteIfExists: Boolean, cliState: CliState) {
        val metrics = MetricList.metricsUsedByPhase(phase, cliState.grade10JobResult.resourceAttributionResult)
        val consumableMetrics = metrics.filterIsInstance<Metric.Consumable>()
                .map { cliState.metricList.metricToIdentifier(it) to it }
                .toMap()
        writeConsumableMetricData(phaseOutputPath, phase, consumableMetrics, overwriteIfExists, cliState)
    }

    private fun writeConsumableMetricData(
            phaseOutputPath: Path,
            phase: Phase,
            consumableMetricsById: Map<String, Metric.Consumable>,
            overwriteIfExists: Boolean,
            cliState: CliState
    ) {
        if (!overwriteIfExists && phaseOutputPath.resolve(CONSUMABLE_RESOURCE_ATTRIBUTION_FILENAME).toFile().exists()) {
            return
        }

        val job = cliState.grade10JobResult

        phaseOutputPath.resolve(CONSUMABLE_RESOURCE_ATTRIBUTION_FILENAME).toFile().bufferedWriter().use { writer ->
            writer.appendln("metric\ttime.slice\tusage\tcapacity")
            consumableMetricsById.toList().sortedBy { it.first }.forEach { (id, metric) ->
                val iter = job.resourceAttributionResult.resourceAttribution[phase].iterator(metric)
                while (iter.hasNext()) {
                    iter.computeNext()
                    writer.apply {
                        append(id)
                        append('\t')
                        append(job.absoluteTimeSliceToRelative(iter.timeSlice).toString())
                        append('\t')
                        append(iter.attributedUsage.toString())
                        append('\t')
                        appendln(iter.availableCapacity.toString())
                    }
                }
            }
        }
    }

}
