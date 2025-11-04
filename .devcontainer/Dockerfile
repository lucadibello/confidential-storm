# syntax=docker/dockerfile:1.4
FROM teaclave/teaclave-java-tee-sdk:v0.1.0-ubuntu18.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG REMOTE_USER=dev
ARG REMOTE_GROUP=${REMOTE_USER}
ARG REMOTE_UID=1010
ARG REMOTE_GID=${REMOTE_UID}
ARG REMOTE_HOME=/home/${REMOTE_USER}
ARG TARGET_ARCH=x64

ARG GIT_EMAIL=lucadibello@proton.me
ARG GIT_NAME="Luca Di Bello"

# Settings for additional tools
ARG GRAALVM_VERSION=22.3.3
ARG OPENJDK_VERSION=17
ARG NEOVIM_VERSION=0.11.4
ARG NODE_VERSION=22.20.0
ARG STORM_VERSION=2.8.2
ARG APACHE_MIRROR=https://dlcdn.apache.org/storm

ENV DEBIAN_FRONTEND=noninteractive \
    NVIM_LISTEN_ADDRESS=127.0.0.1:6666 \
    DEVCONTAINER_USER=${REMOTE_USER} \
    REMOTE_USER=${REMOTE_USER} \
    REMOTE_HOME=${REMOTE_HOME} \
    GIT_NAME=${GIT_NAME} \
    GIT_EMAIL=${GIT_EMAIL} \
    STORM_HOME=/opt/storm \
    GRAALVM_HOME=graalvm-ce-java17-linux-${TARGET_ARCH}-${GRAALVM_VERSION} \
    JAVA_HOME=graalvm-ce-java17-linux-${TARGET_ARCH}-${GRAALVM_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends software-properties-common \
    && add-apt-repository universe \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        # Build & toolchain
        autoconf automake build-essential cmake g++ gcc make zlib1g-dev \
        \
        # VCS
        git \
        \
        # Networking & crypto
        ca-certificates curl openssh-client openssh-server \
        openssl \
        \
        # Archiving & compression
        tar unzip xz-utils \
        \
        # Shells & terminal
        sudo tmux zsh \
        # Newer Java version
        openjdk-${OPENJDK_VERSION}-jdk \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/fdfind /usr/local/bin/fd

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
    && chmod 0440 /etc/sudoers.d/90-"${REMOTE_USER}" \
    && echo "${GROUP_NAME}" > /etc/devcontainer-group

# Update owner of image built-in toolings
RUN chown -R "${REMOTE_USER}:${GROUP_NAME}" /root

# SSH server configuration
RUN mkdir -p /var/run/sshd \
    && ssh-keygen -A \
    && sed -i 's/^#\?Port .*/Port 2222/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PubkeyAuthentication .*/PubkeyAuthentication no/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin no/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?UsePAM .*/UsePAM yes/' /etc/ssh/sshd_config \
    && grep -qxF 'AllowTcpForwarding yes' /etc/ssh/sshd_config || echo 'AllowTcpForwarding yes' >> /etc/ssh/sshd_config \
    && grep -qxF 'X11Forwarding yes' /etc/ssh/sshd_config || echo 'X11Forwarding yes' >> /etc/ssh/sshd_config

# Install ripgrep
RUN curl -fsSL https://github.com/BurntSushi/ripgrep/releases/download/14.1.1/ripgrep_14.1.1-1_amd64.deb -o /tmp/ripgrep.deb \
    && dpkg -i /tmp/ripgrep.deb \
    && rm /tmp/ripgrep.deb

# Install newer GraalVM version (Java 17) for native-image support.
# https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-22.3.3/graalvm-ce-java17-linux-amd64-22.3.3.tar.gz
# NOTE: we install an older version of GraalVM as ubuntu 18 only suppots OpenJDK up to version 17
# NOTE: we only need native-image! GraalVM can coexist with OpenJDK installations.
RUN curl -fsSL "https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-${GRAALVM_VERSION}/graalvm-ce-java17-linux-${TARGET_ARCH}-${GRAALVM_VERSION}.tar.gz" \
  && tar -xzf "graalvm-ce-java17-linux-${TARGET_ARCH}-${GRAALVM_VERSION}.tar.gz" \
  && mv "graalvm-ce-java17-linux-${TARGET_ARCH}-${GRAALVM_VERSION}" /root/tools \
  && /root/tools/graalvm-ce-java17-linux-${TARGET_ARCH}-${GRAALVM_VERSION}/bin/gu install native-image \
  && echo "GraalVM ${GRAALVM_VERSION} installed successfully."

# TODO: Update java_home to point to newer GraalVM version

# NOTE: these are the settings already present inside the /etc/environment file
# GRAALVM_HOME="/root/tools/graalvm-ce-java11-22.2.0"
# JAVA_HOME="/root/tools/graalvm-ce-java11-22.2.0"


# TODO: Set OpenJDK 25 as default Java version using update-alternatives
# NOTE: why? host is compiled using openjdk, enclave using graalvm!
# ➜  confidentialstorm git:(main) java --version
# openjdk 11.0.17 2022-10-18
# OpenJDK Runtime Environment (build 11.0.17+8-post-Ubuntu-1ubuntu218.04)
# OpenJDK 64-Bit Server VM (build 11.0.17+8-post-Ubuntu-1ubuntu218.04, mixed mode, sharing)
# ➜  confidentialstorm git:(main) /root/tools/graalvm-ce-java11-22.2.0/bin/java --version
# openjdk 11.0.16 2022-07-19
# OpenJDK Runtime Environment GraalVM CE 22.2.0 (build 11.0.16+8-jvmci-22.2-b06)
# OpenJDK 64-Bit Server VM GraalVM CE 22.2.0 (build 11.0.16+8-jvmci-22.2-b06, mixed mode, sharing)
# ➜  confidentialstorm git:(main) echo $JAVA_HOME
# /root/tools/graalvm-ce-java11-22.2.0

RUN update-alternatives --set java /usr/lib/jvm/java-17-openjdk-*/bin/java \
  && update-alternatives --set javac /usr/lib/jvm/java-17-openjdk-*/bin/javac \
  


# lazygit (binary release)
RUN LAZYGIT_VERSION=$(curl -s "https://api.github.com/repos/jesseduffield/lazygit/releases/latest" | \grep -Po '"tag_name": *"v\K[^"]*') && \
  curl -Lo lazygit.tar.gz "https://github.com/jesseduffield/lazygit/releases/download/v${LAZYGIT_VERSION}/lazygit_${LAZYGIT_VERSION}_Linux_x86_64.tar.gz" && \
  tar xf lazygit.tar.gz lazygit && \
  install lazygit -D -t /usr/local/bin/ && \
  echo "LazyGit installed successfully."

# Install Node.JS LST (useful for tools!)
RUN curl -fsSLO "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${TARGET_ARCH}.tar.xz" && \
    tar -xf "node-v${NODE_VERSION}-linux-${TARGET_ARCH}.tar.xz" && \
    mv "node-v${NODE_VERSION}-linux-${TARGET_ARCH}" /usr/local/node-v${NODE_VERSION} && \
    ln -sf /usr/local/node-v${NODE_VERSION}/bin/node /usr/local/bin/node && \
    ln -sf /usr/local/node-v${NODE_VERSION}/bin/npm /usr/local/bin/npm && \
    ln -sf /usr/local/node-v${NODE_VERSION}/bin/npx /usr/local/bin/npx && \
    echo "Verifying Node.js installation..." && \
    node -v && npm -v && \
    rm -rf /tmp/node-v${NODE_VERSION}-linux-${TARGET_ARCH}* && \
    echo "Node.js installed successfully."

# Install Neovim 0.11 (nightly channel)
RUN curl -fsSL "https://github.com/neovim/neovim-releases/releases/download/v${NEOVIM_VERSION}/nvim-linux-x86_64.tar.gz" -o /tmp/nvim.tar.gz \
    && tar -xf /tmp/nvim.tar.gz -C /opt \
    && rm /tmp/nvim.tar.gz \
    && mv /opt/nvim-linux-x86_64 /opt/nvim \
    && ln -sf /opt/nvim/bin/nvim /usr/local/bin/nvim

# Provide entrypoint that keeps the headless Neovim server alive
COPY .devcontainer/devcontainer-entrypoint.sh /usr/local/bin/devcontainer-entrypoint
RUN chmod +x /usr/local/bin/devcontainer-entrypoint

# Install apache storm cli
RUN set -eux; \
  curl -fL "${APACHE_MIRROR}/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz" -o /tmp/storm.tgz && \
  tar -xzf /tmp/storm.tgz &&\
  rm -f storm.tgz && \
  ln -s "/opt/apache-storm-${STORM_VERSION}" "${STORM_HOME}" && \
  ln -s "${STORM_HOME}/bin/storm" /usr/local/bin/storm

# Minimal Storm client config (nimbus by service name)
RUN mkdir -p /home/${REMOTE_USER}/.storm && \
    echo 'nimbus.seeds: ["nimbus"]' > /home/${REMOTE_USER}/.storm/storm.yaml

# Install Oh My Zsh for the remote user (non-interactive)
RUN su - "${REMOTE_USER}" -c 'export RUNZSH=no CHSH=no; sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"'

# Ensure cache directory exists for headless Neovim logs
RUN TARGET_GROUP="$(cat /etc/devcontainer-group 2>/dev/null || echo "${REMOTE_GROUP}")" \
    && mkdir -p "${REMOTE_HOME}"/.cache \
    && chown "${REMOTE_USER}:${TARGET_GROUP}" "${REMOTE_HOME}"/.cache

EXPOSE 2222 6666

# set default directory
WORKDIR /workspaces/teaclave-storm

ENTRYPOINT ["/usr/local/bin/devcontainer-entrypoint"]
CMD ["/usr/sbin/sshd", "-D", "-e"]
