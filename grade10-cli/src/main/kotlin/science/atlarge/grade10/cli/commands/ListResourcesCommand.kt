package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.cli.Command
import science.atlarge.grade10.model.execution.Phase

object ListResourcesCommand : Command {

    override val name: String
        get() = "list-resources"
    override val shortHelpMessage: String
        get() = "list the blocking and consumable resources used by a phase"
    override val longHelpMessage: String
        get() = ""

    override fun process(arguments: List<String>, cliState: CliState) {
        when (arguments.size) {
            0 -> listForPhase(cliState.currentPhase, cliState)
            1 -> {
                val phase = cliState.resolvePhaseOrPrintError(arguments[0]) ?: return
                listForPhase(phase, cliState)
            }
            else -> {
                println("Incorrect number of arguments to the \"${name}\" command: ${arguments.size}")
                println("Correct usage: ${name} [path-to-phase]")
            }
        }
    }

    private fun listForPhase(phase: Phase, cliState: CliState) {
        if (phase !in cliState.grade10JobResult.resourceAttributionResult.resourceAttribution.phases) {
            println("Found no resources for phase: ${phase.path}")
        } else {
            val phaseResult = cliState.grade10JobResult.resourceAttributionResult.resourceAttribution[phase]
            if (phaseResult.metrics.isEmpty()) {
                println("Found no resources for phase: ${phase.path}")
            } else {
                println("Listing [B]locking and [C]onsumable resources for phase: ${phase.path}")
                phaseResult.blockingMetrics
                        .map { it.path }
                        .sorted()
                        .forEach { println("    [B] $it") }
                phaseResult.consumableMetrics
                        .map { it.path }
                        .sorted()
                        .forEach { println("    [C] $it") }
            }
        }
    }

}
