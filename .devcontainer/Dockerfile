# syntax=docker/dockerfile:1.4
FROM ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG REMOTE_USER=dev
ARG REMOTE_GROUP=${REMOTE_USER}
ARG REMOTE_UID=1010
ARG REMOTE_GID=${REMOTE_UID}
ARG REMOTE_HOME=/home/${REMOTE_USER}

ARG GIT_EMAIL=lucadibello@proton.me
ARG GIT_NAME="Luca Di Bello"

# Settings for additional tools
ARG GRAALVM_VERSION=22.2.0
ARG GRAALVM_ARCH=amd64
ARG OPENJDK_VERSION=17
ARG NEOVIM_VERSION=0.11.4
ARG NODE_VERSION=22
ARG NODE_ARCH=x64
ARG STORM_VERSION=2.8.3
ARG APACHE_MIRROR=https://dlcdn.apache.org/storm
ARG MAVEN_VERSION=3.9.12

ENV DEBIAN_FRONTEND=noninteractive \
  NVIM_LISTEN_ADDRESS=127.0.0.1:6666 \
  DEVCONTAINER_USER=${REMOTE_USER} \
  REMOTE_USER=${REMOTE_USER} \
  REMOTE_HOME=${REMOTE_HOME} \
  GIT_NAME=${GIT_NAME} \
  GIT_EMAIL=${GIT_EMAIL} \
  STORM_HOME=/opt/storm \
  GRAALVM_HOME=/root/tools/graalvm-ce-java17-${GRAALVM_VERSION} \
  JAVA_HOME=/root/tools/graalvm-ce-java17-${GRAALVM_VERSION}

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
  # fd-find for file searching
  fd-find \
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

# === Install Intel SGX SDK and runtime libraries ===

# Intel SGX runtime libs from Intel apt repo (ubuntu noble = 24.04)
RUN set -eux; \
    install -d -m 0755 /etc/apt/keyrings; \
    curl -fsSL "https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key" -o /etc/apt/keyrings/intel-sgx-keyring.asc; \
    echo 'deb [signed-by=/etc/apt/keyrings/intel-sgx-keyring.asc arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu noble main' > /etc/apt/sources.list.d/intel-sgx.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
    libsgx-launch-dev \
    libsgx-urts \
    libsgx-urts-dbgsym \
    libsgx-uae-service \
    libsgx-enclave-common \
    libsgx-enclave-common-dev \
    libsgx-dcap-ql \
    libsgx-dcap-ql-dev \
    libsgx-dcap-quote-verify-dev \
    libsgx-dcap-default-qpl \
    && rm -rf /var/lib/apt/lists/*
# a) Intel SGX SDK (.bin installer)
RUN set -eux; \
    url="https://download.01.org/intel-sgx/sgx-linux/2.26/distro/ubuntu24.04-server/sgx_linux_x64_sdk_2.26.100.0.bin"; \
    curl -fsSL -o /tmp/sgx_sdk.bin "$url"; \
    chmod +x /tmp/sgx_sdk.bin; \
    mkdir -p /opt/teesdk; \
    /tmp/sgx_sdk.bin --prefix /opt/teesdk; \
    rm -f /tmp/sgx_sdk.bin;
# b) configure environment (avoid sourcing environment file)
ENV SGX_SDK=/opt/teesdk/sgxsdk
ENV PATH="${SGX_SDK}/bin:${SGX_SDK}/bin/x64:${PATH}" \
    PKG_CONFIG_PATH="${SGX_SDK}/pkgconfig:${PKG_CONFIG_PATH}" \
    LD_LIBRARY_PATH="${SGX_SDK}/sdk_libs:${LD_LIBRARY_PATH}"

RUN printf '%s\n' /opt/teesdk/sgxsdk/sdk_libs > /etc/ld.so.conf.d/sgxsdk.conf && ldconfig

# === configure Java toolchain ===
# Install newer GraalVM version (Java 17) for native-image support.
RUN set -eux; \
  mkdir -p /root/tools; \
  url="https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-${GRAALVM_VERSION}/graalvm-ce-java17-linux-${GRAALVM_ARCH}-${GRAALVM_VERSION}.tar.gz"; \
  curl -fsSL -o /tmp/graalvm.tgz "$url"; \
  tar -xzf /tmp/graalvm.tgz -C /root/tools; \
  rm -f /tmp/graalvm.tgz; \
  /root/tools/graalvm-ce-java17-${GRAALVM_VERSION}/bin/gu install native-image; \
  ln -s /root/tools/graalvm-ce-java17-${GRAALVM_VERSION}/bin/native-image /usr/local/bin/native-image; \
  echo "GraalVM ${GRAALVM_VERSION} installed successfully."

# Remove old JAVA_HOME and GRAALVM_HOME from /etc/environment
RUN sed -i '/^GRAALVM_HOME=/d' /etc/environment \
  && sed -i '/^JAVA_HOME=/d' /etc/environment

# Set OpenJDK 17 as default Java version using update-alternatives
RUN update-alternatives --set java /usr/lib/jvm/java-17-openjdk-*/bin/java \
  && update-alternatives --set javac /usr/lib/jvm/java-17-openjdk-*/bin/javac

# Install newer Maven version (to support Java 17)
RUN set -eux; \
  url="https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"; \
  curl -fsSL "$url" -o /tmp/maven.tar.gz; \
  tar -xzf /tmp/maven.tar.gz -C /opt; \
  rm -f /tmp/maven.tar.gz; \
  ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn; \
  echo "Maven ${MAVEN_VERSION} installed successfully."

# === Build and install TeaClave Java TEE SDK ===
# Install teaclave release v0.1.0 to /opt/javaenclave
#COPY --from=docker.io/teaclave/teaclave-java-tee-sdk:v0.1.0-ubuntu18.04 /opt/javaenclave /opt/javaenclave
RUN set -eux; \
    url="https://github.com/apache/teaclave-java-tee-sdk/archive/refs/tags/v0.1.0.tar.gz"; \
    curl -fsSL -o /tmp/javaenclave.tgz "$url"; \
    tar -xzf /tmp/javaenclave.tgz -C /tmp; \
    mvn install:install-file -DgroupId=org.graalvm.compiler -DartifactId=graal-processor -Dversion=22.2.0 -Dpackaging=jar -Dfile="${GRAALVM_HOME}/lib/graal/graal-processor.jar"; \
    mvn -f /tmp/teaclave-java-tee-sdk-0.1.0/sdk/pom.xml clean install; \
    cp -r /tmp/teaclave-java-tee-sdk-0.1.0/sdk/native/bin /opt/javaenclave; \
    cp -r /tmp/teaclave-java-tee-sdk-0.1.0/sdk/native/config /opt/javaenclave; \
    cp -r /tmp/teaclave-java-tee-sdk-0.1.0/sdk/native/script/build_app /opt/javaenclave; \
    rm -rf /tmp/javaenclave.tgz /tmp/teaclave-java-tee-sdk-0.1.0

# === additional tools ===
# Install ripgrep
RUN curl -fsSL https://github.com/BurntSushi/ripgrep/releases/download/14.1.1/ripgrep_14.1.1-1_amd64.deb -o /tmp/ripgrep.deb \
  && dpkg -i /tmp/ripgrep.deb \
  && rm /tmp/ripgrep.deb

# lazygit (binary release)
RUN set -eux; \
  LAZYGIT_VERSION=$(curl -s "https://api.github.com/repos/jesseduffield/lazygit/releases/latest" | \grep -Po '"tag_name": *"v\K[^"]*'); \
  curl -Lo /tmp/lazygit.tar.gz "https://github.com/jesseduffield/lazygit/releases/download/v${LAZYGIT_VERSION}/lazygit_${LAZYGIT_VERSION}_Linux_x86_64.tar.gz"; \
  tar xf /tmp/lazygit.tar.gz -C /tmp lazygit; \
  install /tmp/lazygit -D -t /usr/local/bin/; \
  rm -f /tmp/lazygit.tar.gz /tmp/lazygit; \
  echo "LazyGit installed successfully."

# Install Node.js LTS (useful for tools!)
RUN set -eux; \
  NODE_FULL_VERSION=$(curl -fsSL "https://nodejs.org/dist/latest-v${NODE_VERSION}.x/SHASUMS256.txt" | grep -oP "node-v\K[0-9.]+" | head -1); \
  curl -fsSL "https://nodejs.org/dist/v${NODE_FULL_VERSION}/node-v${NODE_FULL_VERSION}-linux-${NODE_ARCH}.tar.xz" -o /tmp/node.tar.xz; \
  tar -xf /tmp/node.tar.xz -C /usr/local; \
  mv /usr/local/node-v${NODE_FULL_VERSION}-linux-${NODE_ARCH} /usr/local/node; \
  ln -sf /usr/local/node/bin/node /usr/local/bin/node; \
  ln -sf /usr/local/node/bin/npm /usr/local/bin/npm; \
  ln -sf /usr/local/node/bin/npx /usr/local/bin/npx; \
  rm -f /tmp/node.tar.xz; \
  echo "Node.js $(node -v) installed successfully."

# Install Neovim 0.11 (nightly channel)
RUN set -eux; \
  curl -fsSL "https://github.com/neovim/neovim-releases/releases/download/v${NEOVIM_VERSION}/nvim-linux-x86_64.tar.gz" -o /tmp/nvim.tar.gz; \
  tar -xf /tmp/nvim.tar.gz -C /opt; \
  rm /tmp/nvim.tar.gz; \
  mv /opt/nvim-linux-x86_64 /opt/nvim; \
  ln -sf /opt/nvim/bin/nvim /usr/local/bin/nvim

# Install Apache Storm CLI
RUN set -eux; \
  curl -fL "${APACHE_MIRROR}/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz" -o /tmp/storm.tgz; \
  tar -xzf /tmp/storm.tgz -C /opt; \
  rm -f /tmp/storm.tgz; \
  ln -s "/opt/apache-storm-${STORM_VERSION}" "${STORM_HOME}"; \
  ln -s "${STORM_HOME}/bin/storm" /usr/local/bin/storm

# Minimal Storm client config (nimbus by service name)
RUN mkdir -p /home/${REMOTE_USER}/.storm && \
echo 'nimbus.seeds: ["nimbus"]' > /home/${REMOTE_USER}/.storm/storm.yaml

# Install Oh My Zsh for the remote user (non-interactive)
RUN su - "${REMOTE_USER}" -c 'export RUNZSH=no CHSH=no; sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"'

# === External access configuration ===

# SSH server configuration
RUN mkdir -p /var/run/sshd \
    && ssh-keygen -A \
    && sed -i 's/^#\?Port .*/Port 2222/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PubkeyAuthentication .*/PubkeyAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin no/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?UsePAM .*/UsePAM yes/' /etc/ssh/sshd_config \
    && grep -qxF 'AllowTcpForwarding yes' /etc/ssh/sshd_config || echo 'AllowTcpForwarding yes' >> /etc/ssh/sshd_config \
    && grep -qxF 'X11Forwarding yes' /etc/ssh/sshd_config || echo 'X11Forwarding yes' >> /etc/ssh/sshd_config \
    && mkdir -p "${REMOTE_HOME}/.ssh" \
    && chmod 700 "${REMOTE_HOME}/.ssh" \
    && touch "${REMOTE_HOME}/.ssh/authorized_keys" \
    && chmod 600 "${REMOTE_HOME}/.ssh/authorized_keys"

# Provide entrypoint that keeps the headless Neovim server alive
COPY .devcontainer/devcontainer-entrypoint.sh /usr/local/bin/devcontainer-entrypoint
RUN chmod +x /usr/local/bin/devcontainer-entrypoint

# Ensure cache directory exists for headless Neovim logs
RUN TARGET_GROUP="$(cat /etc/devcontainer-group 2>/dev/null || echo "${REMOTE_GROUP}")" \
  && mkdir -p "${REMOTE_HOME}"/.cache \
  && chown "${REMOTE_USER}:${TARGET_GROUP}" "${REMOTE_HOME}"/.cache

# === Final configurations ===
EXPOSE 2222 6666
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD pgrep -x sshd >/dev/null && pgrep -f 'nvim --headless' >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/devcontainer-entrypoint"]
CMD ["/usr/sbin/sshd", "-D", "-e"]
