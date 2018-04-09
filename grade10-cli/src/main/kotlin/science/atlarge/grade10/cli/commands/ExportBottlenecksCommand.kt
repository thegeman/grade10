package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.analysis.bottlenecks.BottleneckStatus
import science.atlarge.grade10.analysis.bottlenecks.BottleneckStatusConstants
import science.atlarge.grade10.analysis.bottlenecks.BottleneckStatusIterator
import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.cli.Command
import science.atlarge.grade10.model.execution.Phase
import java.io.Writer
import java.nio.file.Path

object ExportBottlenecksCommand : Command {

    const val METRIC_BOTTLENECKS_FILENAME = "metric-bottlenecks.tsv"
    const val METRIC_TYPE_BOTTLENECKS_FILENAME = "metric-type-bottlenecks.tsv"

    override val name: String
        get() = "export-bottlenecks"
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

        writeBottleneckData(phaseOutputPath, phase, force, cliState)
    }

    private fun writeBottleneckData(
            phaseOutputPath: Path,
            phase: Phase,
            overwriteIfExists: Boolean,
            cliState: CliState
    ) {
        writeMetricBottleneckData(phaseOutputPath, phase, overwriteIfExists, cliState)
        writeMetricTypeBottleneckData(phaseOutputPath, phase, overwriteIfExists, cliState)
    }

    private fun writeMetricBottleneckData(
            phaseOutputPath: Path,
            phase: Phase,
            overwriteIfExists: Boolean,
            cliState: CliState
    ) {
        if (!overwriteIfExists && phaseOutputPath.resolve(METRIC_BOTTLENECKS_FILENAME).toFile().exists()) {
            return
        }

        val job = cliState.grade10JobResult.bottleneckIdentificationResult.metricBottlenecks[phase]

        phaseOutputPath.resolve(METRIC_BOTTLENECKS_FILENAME).toFile().bufferedWriter().use { writer ->
            writer.appendln("metric\tstart.time.slice\tend.time.slice.inclusive\tbottleneck.status")
            job.metrics.forEach { metric ->
                val id = cliState.metricList.metricToIdentifier(metric)
                val iterator = job.bottleneckIterator(metric)
                writeBottleneckIterator(writer, iterator, id,
                        cliState.grade10JobResult.absoluteTimeSliceToRelative(phase.firstTimeSlice))
            }
        }
    }

    private fun writeMetricTypeBottleneckData(
            phaseOutputPath: Path,
            phase: Phase,
            overwriteIfExists: Boolean,
            cliState: CliState
    ) {
        if (!overwriteIfExists && phaseOutputPath.resolve(METRIC_TYPE_BOTTLENECKS_FILENAME).toFile().exists()) {
            return
        }

        val job = cliState.grade10JobResult.bottleneckIdentificationResult.metricTypeBottlenecks[phase]

        phaseOutputPath.resolve(METRIC_TYPE_BOTTLENECKS_FILENAME).toFile().bufferedWriter().use { writer ->
            writer.appendln("metric.type\tstart.time.slice\tend.time.slice.inclusive\tbottleneck.status")
            job.metricTypes.forEach { metricType ->
                val id = cliState.metricList.metricTypeToIdentifier(metricType)
                val iterator = job.bottleneckIterator(metricType)
                writeBottleneckIterator(writer, iterator, id,
                        cliState.grade10JobResult.absoluteTimeSliceToRelative(phase.firstTimeSlice))
            }
        }
    }

    private fun writeBottleneckIterator(
            writer: Writer,
            iterator: BottleneckStatusIterator,
            metricId: String,
            firstRelativeTimeSlice: Int
    ) {
        if (!iterator.hasNext()) {
            return
        }

        fun writeBottleneck(startTime: Int, endTime: Int, status: BottleneckStatus) {
            writer.apply {
                append(metricId)
                append('\t')
                append(startTime.toString())
                append('\t')
                append(endTime.toString())
                append('\t')
                appendln(when (status) {
                    BottleneckStatusConstants.NONE -> 'N'
                    BottleneckStatusConstants.LOCAL -> 'L'
                    BottleneckStatusConstants.GLOBAL -> 'G'
                    else -> throw IllegalArgumentException("Unknown bottleneck status: $status")
                })
            }
        }

        var startTS = firstRelativeTimeSlice
        var currentStatus = iterator.nextBottleneckStatus()
        var timeSlice = firstRelativeTimeSlice + 1
        while (iterator.hasNext()) {
            val newStatus = iterator.nextBottleneckStatus()
            if (newStatus != currentStatus) {
                writeBottleneck(startTS, timeSlice - 1, currentStatus)
                startTS = timeSlice
                currentStatus = newStatus
            }
            timeSlice++
        }

        writeBottleneck(startTS, timeSlice - 1, currentStatus)
    }

}
