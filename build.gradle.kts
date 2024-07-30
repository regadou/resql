plugins {
    application
    kotlin("jvm") version "1.9.24"
    id("io.ktor.plugin") version "2.3.9"
    id("maven-publish")
}

tasks {
    shadowJar {
        isZip64 = true
    }
}

group = "com.magicreg"
version = "0.1-SNAPSHOT"

kotlin {
    jvmToolchain(11)
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://jitpack.io")
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-reflect:2.0.0")
    implementation("io.ktor:ktor-server-core-jvm")
    implementation("io.ktor:ktor-server-netty-jvm")
    implementation("io.ktor:ktor-server-sessions-jvm")
    implementation("org.jetbrains.kotlin:kotlin-scripting-jsr223")
    implementation("org.jetbrains.kotlin:kotlin-compiler-embeddable")
    implementation("commons-beanutils:commons-beanutils:1.9.4")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.12.3")
    implementation("com.fasterxml.jackson.core:jackson-core:2.12.3")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.3")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.12.3")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.12.3")
    implementation("org.apache.commons:commons-csv:1.10.0")
    implementation("eu.maxschuster:dataurl:2.0.0")
    implementation("com.h2database:h2:2.1.214")
    implementation("mysql:mysql-connector-java:8.0.21")
    implementation("postgresql:postgresql:9.1-901.jdbc4")
    implementation("org.xerial:sqlite-jdbc:3.23.1")
    implementation("org.apache.derby:derby:10.12.1.1")
    implementation("com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11")
    implementation("net.sf.ucanaccess:ucanaccess:5.0.1")
    implementation("com.oracle.database.jdbc:ojdbc11:23.3.0.23.09")
    implementation("org.mongodb:mongodb-driver-sync:5.0.1")
    implementation("org.slf4j:slf4j-nop:2.0.13")
}

application {
    mainClass.set("com.magicreg.resql.MainKt")
}
