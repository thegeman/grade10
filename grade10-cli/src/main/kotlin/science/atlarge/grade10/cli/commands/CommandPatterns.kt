package science.atlarge.grade10.cli.commands

import science.atlarge.grade10.cli.CliState
import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.execution.Phase

fun CliState.resolvePhaseOrPrintError(phasePath: String): Phase? {
    return resolvePhaseOrPrintError(Path.parse(phasePath))
}

fun CliState.resolvePhaseOrPrintError(phasePath: Path): Phase? {
    val phase = currentPhase.resolve(phasePath)
    if (phase == null) {
        println("Cannot find phase: ${currentPhase.path.resolve(phasePath)}")
    }
    return phase
}
