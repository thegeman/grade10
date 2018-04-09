package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.cli.Command
import science.atlarge.grade10.cli.util.MetricList
import science.atlarge.grade10.metrics.TimeSlicePeriodList
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import java.nio.file.Path

object ExportRawMetricDataCommand : Command {

    const val RAW_BLOCKING_METRIC_DATA_FILENAME = "raw-blocking-metric-data.tsv"
    const val RAW_CONSUMABLE_METRIC_DATA_FILENAME = "raw-consumable-metric-data.tsv"

    override val name: String
        get() = "export-raw-metric-data"
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
        val blockingMetrics = metrics.filterIsInstance<Metric.Blocking>()
                .map { cliState.metricList.metricToIdentifier(it) to it }
                .toMap()
        val consumableMetrics = metrics.filterIsInstance<Metric.Consumable>()
                .map { cliState.metricList.metricToIdentifier(it) to it }
                .toMap()
        writeBlockingMetricData(phaseOutputPath, phase, blockingMetrics, overwriteIfExists, cliState)
        writeConsumableMetricData(phaseOutputPath, phase, consumableMetrics, overwriteIfExists, cliState)
    }

    private fun writeBlockingMetricData(
            phaseOutputPath: Path,
            phase: Phase,
            blockingMetricsById: Map<String, Metric.Blocking>,
            overwriteIfExists: Boolean,
            cliState: CliState
    ) {
        if (!overwriteIfExists && phaseOutputPath.resolve(RAW_BLOCKING_METRIC_DATA_FILENAME).toFile().exists()) {
            return
        }

        val job = cliState.grade10JobResult
        val startTimeSlice = phase.firstTimeSlice
        val endTimeSlice = phase.lastTimeSlice

        phaseOutputPath.resolve(RAW_BLOCKING_METRIC_DATA_FILENAME).toFile().bufferedWriter().use { writer ->
            writer.appendln("metric\tstart.time.slice\tend.time.slice.inclusive\tis.blocked")
            blockingMetricsById.toList().sortedBy { it.first }.forEach { (id, metric) ->
                val blockedPeriods = metric.blockedTimeSlices.periods
                        .filter {
                            it.first <= endTimeSlice && it.endInclusive >= startTimeSlice
                        }
                        .map { it to true }
                val unblockedPeriods = (TimeSlicePeriodList(phase.timeSliceRange) - metric.blockedTimeSlices).periods
                        .map { it to false }
                val sortedPeriods = (blockedPeriods + unblockedPeriods).sortedBy { it.first.first }
                sortedPeriods.forEach { (period, isBlocked) ->
                    writer.apply {
                        append(id)
                        append('\t')
                        append(job.absoluteTimeSliceToRelative(period.first).toString())
                        append('\t')
                        append(job.absoluteTimeSliceToRelative(period.last).toString())
                        append('\t')
                        appendln(if (isBlocked) '1' else '0')
                    }
                }
            }
        }
    }

    private fun writeConsumableMetricData(
            phaseOutputPath: Path,
            phase: Phase,
            consumableMetricsById: Map<String, Metric.Consumable>,
            overwriteIfExists: Boolean,
            cliState: CliState
    ) {
        if (!overwriteIfExists && phaseOutputPath.resolve(RAW_CONSUMABLE_METRIC_DATA_FILENAME).toFile().exists()) {
            return
        }

        val job = cliState.grade10JobResult
        val startTimeSlice = phase.firstTimeSlice
        val endTimeSlice = phase.lastTimeSlice

        phaseOutputPath.resolve(RAW_CONSUMABLE_METRIC_DATA_FILENAME).toFile().bufferedWriter().use { writer ->
            writer.appendln("metric\tstart.time.slice\tend.time.slice.inclusive\tusage")
            consumableMetricsById.toList().sortedBy { it.first }.forEach { (id, metric) ->
                val iter = metric.observedUsage.observationIteratorForTimeSlices(startTimeSlice, endTimeSlice)
                while (iter.hasNext()) {
                    iter.nextObservationPeriod()
                    writer.apply {
                        append(id)
                        append('\t')
                        append(job.absoluteTimeSliceToRelative(iter.periodStartTimeSlice).toString())
                        append('\t')
                        append(job.absoluteTimeSliceToRelative(iter.periodEndTimeSlice).toString())
                        append('\t')
                        appendln(iter.observation.toString())
                    }
                }
            }
        }
    }

}
