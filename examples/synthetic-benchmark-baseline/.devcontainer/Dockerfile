# syntax=docker/dockerfile:1.4
FROM ubuntu:22.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG REMOTE_USER=dev
ARG REMOTE_GROUP=${REMOTE_USER}
ARG REMOTE_UID=1010
ARG REMOTE_GID=${REMOTE_UID}
ARG REMOTE_HOME=/home/${REMOTE_USER}

ARG GIT_EMAIL=lucadibello@proton.me
ARG GIT_NAME="Luca Di Bello"

ARG OPENJDK_VERSION=17
ARG MAVEN_VERSION=3.9.12
ARG STORM_VERSION=2.8.3
ARG APACHE_MIRROR=https://dlcdn.apache.org/storm

ENV DEBIAN_FRONTEND=noninteractive \
    DEVCONTAINER_USER=${REMOTE_USER} \
    REMOTE_USER=${REMOTE_USER} \
    REMOTE_HOME=${REMOTE_HOME} \
    GIT_NAME=${GIT_NAME} \
    GIT_EMAIL=${GIT_EMAIL} \
    STORM_HOME=/opt/storm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    # Build & toolchain
    build-essential \
    \
    # VCS
    git \
    \
    # Networking & crypto
    ca-certificates curl \
    \
    # Archiving
    tar unzip \
    \
    # Shell & terminal
    sudo zsh \
    \
    # Java
    openjdk-${OPENJDK_VERSION}-jdk \
    \
    # Python
    python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user with passwordless sudo
RUN GROUP_NAME="${REMOTE_GROUP}" \
    && EXISTING_GID_GROUP="$(getent group "${REMOTE_GID}" | cut -d: -f1 || true)" \
    && if [ -n "${EXISTING_GID_GROUP}" ]; then \
    GROUP_NAME="${EXISTING_GID_GROUP}"; \
    elif getent group "${REMOTE_GROUP}" >/dev/null; then \
    groupmod --gid "${REMOTE_GID}" "${REMOTE_GROUP}"; \
    else \
    groupadd --gid "${REMOTE_GID}" "${REMOTE_GROUP}"; \
    fi \
    && EXISTING_UID_USER="$(getent passwd "${REMOTE_UID}" | cut -d: -f1 || true)" \
    && if ! id -u "${REMOTE_USER}" >/dev/null 2>&1 \
    && [ -n "${EXISTING_UID_USER}" ] \
    && [ "${EXISTING_UID_USER}" != "${REMOTE_USER}" ]; then \
    usermod --login "${REMOTE_USER}" "${EXISTING_UID_USER}"; \
    fi \
    && if id -u "${REMOTE_USER}" >/dev/null 2>&1; then \
    usermod --gid "${GROUP_NAME}" --shell /usr/bin/zsh "${REMOTE_USER}"; \
    CURRENT_UID="$(id -u "${REMOTE_USER}")"; \
    if [ "${CURRENT_UID}" -ne "${REMOTE_UID}" ] \
    && { [ -z "${EXISTING_UID_USER}" ] || [ "${EXISTING_UID_USER}" = "${REMOTE_USER}" ]; }; then \
    usermod --uid "${REMOTE_UID}" "${REMOTE_USER}"; \
    fi; \
    CURRENT_HOME="$(getent passwd "${REMOTE_USER}" | cut -d: -f6)"; \
    if [ "${CURRENT_HOME}" != "${REMOTE_HOME}" ]; then \
    usermod --home "${REMOTE_HOME}" --move-home "${REMOTE_USER}"; \
    fi; \
    else \
    useradd --create-home --shell /usr/bin/zsh --uid "${REMOTE_UID}" --gid "${GROUP_NAME}" "${REMOTE_USER}"; \
    fi \
    && usermod -aG sudo "${REMOTE_USER}" \
    && echo "${REMOTE_USER} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/90-"${REMOTE_USER}" \
    && chmod 0440 /etc/sudoers.d/90-"${REMOTE_USER}"

# Install Maven
RUN set -eux; \
    curl -fsSL "https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
    -o /tmp/maven.tar.gz; \
    tar -xzf /tmp/maven.tar.gz -C /opt; \
    rm /tmp/maven.tar.gz; \
    ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn; \
    echo "Maven ${MAVEN_VERSION} installed."

# Install Apache Storm CLI
RUN set -eux; \
    curl -fL "${APACHE_MIRROR}/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz" \
    -o /tmp/storm.tgz; \
    tar -xzf /tmp/storm.tgz -C /opt; \
    rm /tmp/storm.tgz; \
    ln -s "/opt/apache-storm-${STORM_VERSION}" "${STORM_HOME}"; \
    ln -s "${STORM_HOME}/bin/storm" /usr/local/bin/storm; \
    echo "Storm ${STORM_VERSION} installed."

# Set JAVA_HOME to the installed OpenJDK
RUN JAVA_PATH="$(update-java-alternatives -l 2>/dev/null | awk '{print $3}' | head -1)" \
    && echo "JAVA_HOME=${JAVA_PATH}" >> /etc/environment \
    && echo "export JAVA_HOME=${JAVA_PATH}" >> /etc/profile.d/java.sh

# Create Storm client config directory
RUN mkdir -p /home/${REMOTE_USER}/.storm

COPY examples/synthetic-benchmark-baseline/.devcontainer/devcontainer-entrypoint.sh /usr/local/bin/devcontainer-entrypoint
RUN chmod +x /usr/local/bin/devcontainer-entrypoint

EXPOSE 2222

ENTRYPOINT ["/usr/local/bin/devcontainer-entrypoint"]
CMD ["bash"]
