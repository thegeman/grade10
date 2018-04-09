package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.cli.Command
import science.atlarge.grade10.model.execution.Phase
import java.nio.file.Path

object ExportPerformanceIssuesCommand : Command {

    const val PERFORMANCE_ISSUES_FILENAME = "performance-issues.tsv"

    override val name: String
        get() = "export-performance-issues"
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

        writePerformanceIssueData(phaseOutputPath, phase, force, cliState)
    }

    private fun writePerformanceIssueData(
            phaseOutputPath: Path,
            phase: Phase,
            overwriteIfExists: Boolean,
            cliState: CliState
    ) {
        if (!overwriteIfExists && phaseOutputPath.resolve(PERFORMANCE_ISSUES_FILENAME).toFile().exists()) {
            return
        }

        val issues = cliState.grade10JobResult.performanceIssueIdentificationResult.resultsDisplayedAt(phase)
                .sortedByDescending { it.estimatedImpact }

        phaseOutputPath.resolve(PERFORMANCE_ISSUES_FILENAME).toFile().bufferedWriter().use { writer ->
            writer.appendln("estimated.impact\tperformance.issue")
            issues.forEach { issue ->
                writer.apply {
                    append(issue.estimatedImpact.toString())
                    append('\t')
                    appendln(issue.toDisplayString())
                }
            }
        }
    }

}
