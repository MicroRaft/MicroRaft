plugins {
    alias(libs.plugins.spotbugs) apply false
}

// Checkstyle and Spotbugs are applied for every project but do need to be triggered manually
allprojects {
    plugins.apply("checkstyle")
    plugins.apply("com.github.spotbugs")
}