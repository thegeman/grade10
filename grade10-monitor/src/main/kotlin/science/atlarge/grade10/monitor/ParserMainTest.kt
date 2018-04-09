package science.atlarge.grade10.monitor

import java.nio.file.Paths
import kotlin.system.measureNanoTime

fun main(args: Array<String>) {
    val appDir = Paths.get("D:/syncthing/tudelft/research/msc-thesis/test-results/20170130-10workers-32cores/")
    val logDir = appDir.resolve("resource-monitor")

    var monitorOutput: MonitorOutput? = null
    val parseTime = measureNanoTime {
        monitorOutput = MonitorOutputParser.parseDirectory(logDir)
    }

    val writeTime = measureNanoTime {
        monitorOutput!!.writeToFile(appDir.resolve("monitor-output.bin").toFile())
    }

    var newMonitorOutput: MonitorOutput? = null
    val readTime = measureNanoTime {
        newMonitorOutput = MonitorOutput.readFromFile(appDir.resolve("monitor-output.bin").toFile())
    }

    println("Parse: $parseTime ns")
    println("Write: $writeTime ns")
    println("Read: $readTime ns")

    println(newMonitorOutput!!.machines.size)
}
