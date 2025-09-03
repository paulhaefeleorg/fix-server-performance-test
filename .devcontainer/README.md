# Development Container Setup

This development container provides a complete Java 21 environment for FIX performance testing with all required dependencies.

## Features

- **Java 21** (Eclipse Temurin)
- **Gradle** build system
- **VS Code Java extensions** pre-installed
- **Google Java Style** formatting configured
- **Performance-optimized JVM settings**

## Getting Started

1. **Open in Dev Container**: Use VS Code's "Reopen in Container" command
2. **Or use Docker Compose**: Run `docker-compose up -d` to start the development environment
3. **Build the project**: `./gradlew build`
4. **Run tests**: `./gradlew test`

## Available Tasks

- `./gradlew runGenerate` - Run FIX message generator
- `./gradlew runFlyweight` - Run flyweight consumer
- `./gradlew runQuickFIXJ` - Run QuickFIX/J consumer

## Directory Structure

- `data/` - Chronicle Queue data files
- `metrics/` - Performance metrics output
- `logs/` - Application logs
- `scripts/` - Shell scripts for running tests

## Environment Variables

- `JAVA_HOME` - Set to `/opt/java/openjdk`
- `GRADLE_OPTS` - Optimized for performance testing
- `JAVA_OPTS` - G1GC with low pause times

## Ports

- `8080` - FIX Generator
- `8081` - Flyweight Consumer  
- `8082` - QuickFIX/J Consumer
