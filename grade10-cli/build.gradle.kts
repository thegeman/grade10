plugins {
    kotlin("jvm")
    application
}

application {
    mainClassName = "science.atlarge.grade10.cli.CliKt"
}

tasks {
    "run"(JavaExec::class) {
        standardInput = System.`in`
    }
}

dependencies {
    compile(kotlin("stdlib"))

    compile(project(":grade10-core"))
    compile(project(":grade10-examples"))
}