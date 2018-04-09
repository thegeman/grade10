package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.cli.Command
import science.atlarge.grade10.model.execution.Phase

object ListCommand : Command {

    override val name: String
        get() = "list"
    override val shortHelpMessage: String
        get() = "list available subphases"
    override val longHelpMessage: String
        get() = ""

    override fun process(arguments: List<String>, cliState: CliState) {
        when (arguments.size) {
            0 -> listForPhase(cliState.currentPhase)
            1 -> {
                val phase = cliState.resolvePhaseOrPrintError(arguments[0]) ?: return
                listForPhase(phase)
            }
            else -> {
                println("Incorrect number of arguments to the \"${name}\" command: ${arguments.size}")
                println("Correct usage: ${name} [path-to-phase]")
            }
        }
    }

    private fun listForPhase(phase: Phase) {
        val subphases = phase.subphasesByShortName.keys.sorted()
        if (subphases.isEmpty()) {
            println("No subphases found for phase: ${phase.path}")
        } else {
            println("Listing subphases of phase: ${phase.path}")
            subphases.forEach { subphase ->
                println("\t$subphase")
            }
        }
    }

}
