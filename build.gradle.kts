plugins {
    `java-library`
    `maven-publish`
    signing
    id("com.gradleup.nmcp").version("1.4.4")
    id("com.gradleup.nmcp.aggregation").version("1.4.4")
}

group = "io.github.merlimat.concurrent.queue"
version = System.getenv("RELEASE_VERSION") ?: "0.0.0-SNAPSHOT"

val junitVersion: String by project

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withJavadocJar()
    withSourcesJar()
}

tasks.withType<JavaCompile>().configureEach {
    // Compile against the Java 17 API, whatever JDK runs the build
    options.release = 17
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    nmcpAggregation(project(":"))
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "concurrent-collections"

            pom {
                name = "concurrent-collections"
                description = "Concurrent data structures for Java"
                url = "https://github.com/merlimat/concurrent-collections"

                licenses {
                    license {
                        name = "Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0"
                    }
                }

                developers {
                    developer {
                        id = "merlimat"
                        name = "Matteo Merli"
                        email = "matteo.merli@gmail.com"
                    }
                }

                scm {
                    connection = "scm:git:git://github.com/merlimat/concurrent-collections.git"
                    developerConnection = "scm:git:ssh://github.com/merlimat/concurrent-collections.git"
                    url = "https://github.com/merlimat/concurrent-collections"
                }
            }
        }
    }
}

signing {
    val signingKey = System.getenv("GPG_PRIVATE_KEY")
    val signingPassword = System.getenv("GPG_PASSPHRASE")
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    }
}

nmcpAggregation {
    centralPortal {
        username = System.getenv("CENTRAL_USERNAME")
        password = System.getenv("CENTRAL_PASSWORD")
        publishingType = "AUTOMATIC"
    }
}
