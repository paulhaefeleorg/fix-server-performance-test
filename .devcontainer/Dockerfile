FROM eclipse-temurin:21-jdk

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    git \
    build-essential \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Create workspace directory
RUN mkdir -p /workspaces/fix-server-performance-test

# Set working directory
WORKDIR /workspaces/fix-server-performance-test

# Create necessary directories for the project
RUN mkdir -p data metrics logs scripts

# Set environment variables
ENV JAVA_HOME=/opt/java/openjdk
ENV GRADLE_OPTS="-Xmx2g -XX:+UseG1GC"
ENV JAVA_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Create a non-root user with proper UID/GID for dev containers
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    && chown -R $USERNAME:$USERNAME /workspaces/fix-server-performance-test

# Install Gradle
RUN curl -L -o /tmp/gradle.zip https://services.gradle.org/distributions/gradle-8.5-bin.zip \
    && unzip /tmp/gradle.zip -d /opt \
    && ln -s /opt/gradle-8.5/bin/gradle /usr/local/bin/gradle \
    && rm /tmp/gradle.zip

# Debug: Show user info
RUN echo "Created user: $USERNAME" && id $USERNAME || echo "User creation failed"

# Switch to the user
USER $USERNAME
