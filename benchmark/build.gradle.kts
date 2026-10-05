plugins {
    application
}

val hdrHistogramVersion: String by project

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":"))
    implementation("org.hdrhistogram:HdrHistogram:$hdrHistogramVersion")
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 17
}

application {
    mainClass = "io.github.merlimat.queues.benchmark.QueueLatencyBenchmark"
}
