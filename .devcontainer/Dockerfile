# syntax=docker/dockerfile:1.4
FROM teaclave/teaclave-java-tee-sdk:v0.1.0-ubuntu18.04

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
ARG NODE_VERSION=22.20.0
ARG NODE_ARCH=x64
ARG STORM_VERSION=2.8.2
ARG APACHE_MIRROR=https://archive.apache.org/dist/storm
ARG MAVEN_VERSION=3.9.12

ENV DEBIAN_FRONTEND=noninteractive \
  DEVCONTAINER_USER=${REMOTE_USER} \
  REMOTE_USER=${REMOTE_USER} \
  REMOTE_HOME=${REMOTE_HOME} \
  GIT_NAME=${GIT_NAME} \
  GIT_EMAIL=${GIT_EMAIL} \
  STORM_HOME=/opt/storm \
  GRAALVM_HOME=/root/tools/graalvm-ce-java17-${GRAALVM_VERSION} \
  JAVA_HOME=/root/tools/graalvm-ce-java17-${GRAALVM_VERSION}

RUN rm -f /etc/apt/sources.list.d/*sgx* /etc/apt/sources.list.d/*intel* \
  && apt-get update \
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
  openssl rsync \
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

# SSH server configuration
RUN mkdir -p /var/run/sshd \
  && ssh-keygen -A \
  && sed -i 's/^#\?Port .*/Port 2222/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?PubkeyAuthentication .*/PubkeyAuthentication yes/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin no/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?UsePAM .*/UsePAM yes/' /etc/ssh/sshd_config \
  && grep -qxF 'AllowTcpForwarding yes' /etc/ssh/sshd_config || echo 'AllowTcpForwarding yes' >> /etc/ssh/sshd_config \
  && grep -qxF 'X11Forwarding yes' /etc/ssh/sshd_config || echo 'X11Forwarding yes' >> /etc/ssh/sshd_config

# Install newer GraalVM version (Java 17) for native-image support.
RUN set -eux; \
  url="https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-${GRAALVM_VERSION}/graalvm-ce-java17-linux-${GRAALVM_ARCH}-${GRAALVM_VERSION}.tar.gz"; \
  curl -fsSL -o /tmp/graalvm.tgz "$url"; \
  tar -xzf /tmp/graalvm.tgz -C /root/tools; \
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
  url="https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"; \
  curl -fsSL "$url" -o /tmp/maven.tar.gz; \
  tar -xzf /tmp/maven.tar.gz -C /opt; \
  ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn; \
  echo "Maven ${MAVEN_VERSION} installed successfully.";

# lazygit (binary release)
RUN set -eux; \
  LAZYGIT_VERSION=$(curl -s "https://api.github.com/repos/jesseduffield/lazygit/releases/latest" | \grep -Po '"tag_name": *"v\K[^"]*'); \
  curl -Lo lazygit.tar.gz "https://github.com/jesseduffield/lazygit/releases/download/v${LAZYGIT_VERSION}/lazygit_${LAZYGIT_VERSION}_Linux_x86_64.tar.gz"; \
  tar xf lazygit.tar.gz lazygit; \
  install lazygit -D -t /usr/local/bin/; \
  echo "LazyGit installed successfully."

# Install Node.JS LST (useful for tools!)
RUN set -eux; \
  curl -fsSLO "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz"; \
  tar -xf "node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz"; \
  mv "node-v${NODE_VERSION}-linux-${NODE_ARCH}" /usr/local/node-v${NODE_VERSION}; \
  ln -sf /usr/local/node-v${NODE_VERSION}/bin/node /usr/local/bin/node; \
  ln -sf /usr/local/node-v${NODE_VERSION}/bin/npm /usr/local/bin/npm; \
  ln -sf /usr/local/node-v${NODE_VERSION}/bin/npx /usr/local/bin/npx; \
  echo "Verifying Node.js installation..."; \
  node -v; npm -v; \
  rm -rf /tmp/node-v${NODE_VERSION}-linux-${NODE_ARCH}*; \
  echo "Node.js installed successfully."

# Provide entrypoint
COPY .devcontainer/devcontainer-entrypoint.sh /usr/local/bin/devcontainer-entrypoint
RUN chmod +x /usr/local/bin/devcontainer-entrypoint

# Install apache storm cli
RUN set -eux; \
  curl -fL "${APACHE_MIRROR}/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz" -o /tmp/storm.tgz; \
  tar -xzf /tmp/storm.tgz; \
  rm -f storm.tgz; \
  ln -s "/opt/apache-storm-${STORM_VERSION}" "${STORM_HOME}"; \
  ln -s "${STORM_HOME}/bin/storm" /usr/local/bin/storm

# Remove the SDK bundled in the base image and rebuild from the fork
RUN set -eux; \
  rm -rf /opt/javaenclave; \
  git clone https://github.com/lucadibello/teaclave-java-tee-sdk.git /tmp/teaclave-sdk; \
  cd /tmp/teaclave-sdk; \
  mvn install:install-file \
    -DgroupId=org.graalvm.compiler \
    -DartifactId=graal-processor \
    -Dversion=${GRAALVM_VERSION} \
    -Dpackaging=jar \
    -Dfile="${GRAALVM_HOME}/lib/graal/graal-processor.jar"; \
  export JAVA_HOME="${GRAALVM_HOME}"; \
  mkdir -p /opt/javaenclave; \
  mvn -f third-party-libs/bouncycastle-native/pom.xml clean install -DskipTests; \
  mvn -f sdk/pom.xml clean install -DskipTests; \
  cp -r sdk/native/bin     /opt/javaenclave/; \
  cp -r sdk/native/config  /opt/javaenclave/; \
  cp -r sdk/native/stubs   /opt/javaenclave/; \
  cp -r sdk/native/script/build_app /opt/javaenclave/; \
  rm -rf /tmp/teaclave-sdk; \
  echo "TeaClave SDK build complete."

# Install Docker CLI + Compose v2 plugin (DooD: socket bind-mounted at runtime via devcontainer.json).
# Static binaries are used instead of the apt repo because Ubuntu 18.04 only
# has Docker 24 (API 1.43) which is too old for Docker 25+ daemons (min API 1.44).
ARG DOCKER_VERSION=27.5.1
ARG DOCKER_COMPOSE_VERSION=2.29.7
RUN set -eux; \
  curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" \
    | tar -xz --strip-components=1 -C /usr/local/bin docker/docker; \
  mkdir -p /usr/local/lib/docker/cli-plugins; \
  curl -fsSL "https://github.com/docker/compose/releases/download/v${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose; \
  chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Hand ownership of /root (tools + Maven cache installed there by root) to the dev user
RUN chown -R "${REMOTE_USER}:" /root

# Create Storm client config directory (storm.yaml is bind-mounted at runtime via devcontainer.json)
RUN mkdir -p /home/${REMOTE_USER}/.storm

# Install Oh My Zsh for the remote user (non-interactive)
RUN su - "${REMOTE_USER}" -c 'export RUNZSH=no CHSH=no; sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"'

EXPOSE 2222

ENTRYPOINT ["/usr/local/bin/devcontainer-entrypoint"]
CMD ["/usr/sbin/sshd", "-D", "-e"]
