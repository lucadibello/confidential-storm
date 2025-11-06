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
ARG NODE_VERSION=22.20.0
ARG NODE_ARCH=x64
ARG STORM_VERSION=2.8.2
ARG APACHE_MIRROR=https://dlcdn.apache.org/storm
ARG MAVEN_VERSION=3.9.11

ENV DEBIAN_FRONTEND=noninteractive \
  NVIM_LISTEN_ADDRESS=127.0.0.1:6666 \
  DEVCONTAINER_USER=${REMOTE_USER} \
  REMOTE_USER=${REMOTE_USER} \
  REMOTE_HOME=${REMOTE_HOME} \
  GIT_NAME=${GIT_NAME} \
  GIT_EMAIL=${GIT_EMAIL} \
  STORM_HOME=/opt/storm \
  GRAALVM_HOME=/root/tools/graalvm-ce-java17-${GRAALVM_VERSION} \
  JAVA_HOME=/root/tools/graalvm-ce-java17-${GRAALVM_VERSION} \
  SGX_SDK=/opt/sgxsdk \
  PKG_CONFIG_PATH=/opt/sgxsdk/pkgconfig:$PKG_CONFIG_PATH \
  LD_LIBRARY_PATH=/opt/sgxsdk/sdk_libs:$LD_LIBRARY_PATH

# 0) Core setup
RUN apt-get update && apt-get install -y --no-install-recommends \
  autoconf automake build-essential cmake g++ gcc make ocaml ocamlbuild zlib1g-dev python-is-python3 pkg-config perl file \
  git \
  ca-certificates curl openssh-client openssh-server openssl wget gnupg jq \
  tar unzip xz-utils \
  sudo tmux zsh \
  openjdk-${OPENJDK_VERSION}-jdk \
  libssl-dev libtool \
  && rm -rf /var/lib/apt/lists/*

# a) Create non-root user with passwordless sudo + matching UID/GID of host user
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

# b) Intel SGX runtime libs from Intel apt repo (noble)
RUN set -eux; \
  install -d -m 0755 /etc/apt/keyrings; \
  curl -fsSL "https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key" -o /etc/apt/keyrings/intel-sgx-keyring.asc; \
  echo 'deb [signed-by=/etc/apt/keyrings/intel-sgx-keyring.asc arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu noble main' > /etc/apt/sources.list.d/intel-sgx.list; \
  # curl -fsSL "https://occlum.io/occlum-package-repos/debian/public.key" -o /etc/apt/keyrings/occulum-keyring.asc; \
  # echo 'deb [signed-by=/etc/apt/keyrings/occulum-keyring.asc arch=amd64] https://occlum.io/occlum-package-repos/debian noble main' > /etc/apt/sources.list.d/occulum.list; \
  apt-get update; \
  apt-get install -y --no-install-recommends \
  # occulum \
  libsgx-launch-dev \
  libsgx-urts \
  libsgx-uae-service \
  libsgx-dcap-ql \
  libsgx-dcap-ql-dev \
  libsgx-dcap-quote-verify-dev \
  libsgx-dcap-default-qpl \
  sgx-aesm-service \
  && rm -rf /var/lib/apt/lists/*

# c) Install occulum from source
RUN set -eux; \
  git clone https://github.com/occlum/occlum /tmp/occlum-src; \
  cd /tmp/occlum-src; \
  OCCLUM_RELEASE_BUILD=1 make submodule; \
  OCCLUM_RELEASE_BUILD=1 make; \
  OCCLUM_RELEASE_BUILD=1 make install;

# d) Copy occulum DCAP libraries locally for remote attestation support
# COPY --from=docker.io/occlum/occlum:0.31.0-ubuntu22.04 /opt/occlum/toolchains/dcap_lib /opt/occlum/toolchains/dcap_lib

# 1) Install core components for development
RUN mkdir -p /root/tools

# a) Intel SGX SDK (.bin installer)
RUN set -eux; \
  url="https://download.01.org/intel-sgx/sgx-linux/2.26/distro/ubuntu24.04-server/sgx_linux_x64_sdk_2.26.100.0.bin"; \
  curl -fsSL -o /tmp/sgx_sdk.bin "$url"; \
  chmod +x /tmp/sgx_sdk.bin; \
  /tmp/sgx_sdk.bin --prefix /opt; \
  rm -f /tmp/sgx_sdk.bin;

# b) GraalVM (Java 17) + native-image
RUN set -eux; \
  url="https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-${GRAALVM_VERSION}/graalvm-ce-java17-linux-${GRAALVM_ARCH}-${GRAALVM_VERSION}.tar.gz"; \
  curl -fsSL -o /tmp/graalvm.tgz "$url"; \
  tar -xzf /tmp/graalvm.tgz -C /root/tools; \
  /root/tools/graalvm-ce-java17-${GRAALVM_VERSION}/bin/gu install native-image; \
  ln -sf /root/tools/graalvm-ce-java17-${GRAALVM_VERSION}/bin/native-image /usr/local/bin/native-image; \
  rm -f /tmp/graalvm.tgz; \
  echo "GraalVM ${GRAALVM_VERSION} installed successfully."

# 2) Additional tools

# a) ripgrep
RUN set -eux; \
  curl -fsSL https://github.com/BurntSushi/ripgrep/releases/download/14.1.1/ripgrep_14.1.1-1_amd64.deb -o /tmp/ripgrep.deb; \
  dpkg -i /tmp/ripgrep.deb || apt-get -y --no-install-recommends -f install; \
  rm -f /tmp/ripgrep.deb

# b) Lazygit
RUN set -eux; \
  LAZYGIT_VERSION=$(curl -s "https://api.github.com/repos/jesseduffield/lazygit/releases/latest" | grep -Po '"tag_name": *"v\K[^"]*'); \
  curl -fsSL -o /tmp/lazygit.tar.gz "https://github.com/jesseduffield/lazygit/releases/download/v${LAZYGIT_VERSION}/lazygit_${LAZYGIT_VERSION}_Linux_x86_64.tar.gz"; \
  tar -xf /tmp/lazygit.tar.gz -C /tmp lazygit; \
  install /tmp/lazygit -D -t /usr/local/bin/; \
  rm -f /tmp/lazygit /tmp/lazygit.tar.gz; \
  echo "LazyGit installed successfully."

# c) Node.js
RUN set -eux; \
  curl -fsSLO "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz"; \
  tar -xf "node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz"; \
  mv "node-v${NODE_VERSION}-linux-${NODE_ARCH}" /usr/local/node-v${NODE_VERSION}; \
  ln -sf /usr/local/node-v${NODE_VERSION}/bin/node /usr/local/bin/node; \
  ln -sf /usr/local/node-v${NODE_VERSION}/bin/npm /usr/local/bin/npm; \
  ln -sf /usr/local/node-v${NODE_VERSION}/bin/npx /usr/local/bin/npx; \
  node -v; npm -v; \
  rm -f "node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz"; \
  echo "Node.js installed successfully."

# d) Neovim
RUN set -eux; \
  curl -fsSL "https://github.com/neovim/neovim-releases/releases/download/v${NEOVIM_VERSION}/nvim-linux-x86_64.tar.gz" -o /tmp/nvim.tar.gz; \
  tar -xf /tmp/nvim.tar.gz -C /opt; \
  rm /tmp/nvim.tar.gz; \
  mv /opt/nvim-linux-x86_64 /opt/nvim; \
  ln -sf /opt/nvim/bin/nvim /usr/local/bin/nvim

# e) Apache Storm CLI (fixed extraction path)
RUN set -eux; \
  curl -fsSL "${APACHE_MIRROR}/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz" -o /tmp/storm.tgz; \
  tar -xzf /tmp/storm.tgz -C /opt; \
  rm -f /tmp/storm.tgz; \
  ln -s "/opt/apache-storm-${STORM_VERSION}" "${STORM_HOME}"; \
  ln -sf "${STORM_HOME}/bin/storm" /usr/local/bin/storm

# f) Oh My Zsh for the remote user
RUN su - "${REMOTE_USER}" -c 'export RUNZSH=no CHSH=no; sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"'

# 3) Additional configurations

# a) Make OpenJDK 17 the default javac/java (you still use GraalVM for native-image)
RUN update-alternatives --set java /usr/lib/jvm/java-17-openjdk-*/bin/java \
  && update-alternatives --set javac /usr/lib/jvm/java-17-openjdk-*/bin/javac

# b) Devcontainer entrypoint
COPY .devcontainer/devcontainer-entrypoint.sh /usr/local/bin/devcontainer-entrypoint
RUN chmod +x /usr/local/bin/devcontainer-entrypoint

# c) Minimal Storm client config
RUN mkdir -p /home/${REMOTE_USER}/.storm \
  && echo 'nimbus.seeds: ["nimbus"]' > /home/${REMOTE_USER}/.storm/storm.yaml \
  && chown -R ${REMOTE_USER}:${REMOTE_GROUP} /home/${REMOTE_USER}/.storm

# d) Ensure cache dir for Neovim logs
RUN TARGET_GROUP="$(cat /etc/devcontainer-group 2>/dev/null || echo "${REMOTE_GROUP}")" \
  && mkdir -p "${REMOTE_HOME}"/.cache \
  && chown "${REMOTE_USER}:${TARGET_GROUP}" "${REMOTE_HOME}"/.cache

# f) SSH server configuration
RUN mkdir -p /var/run/sshd \
  && ssh-keygen -A \
  && sed -i 's/^#\?Port .*/Port 2222/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?PubkeyAuthentication .*/PubkeyAuthentication no/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin no/' /etc/ssh/sshd_config \
  && sed -i 's/^#\?UsePAM .*/UsePAM yes/' /etc/ssh/sshd_config \
  && grep -qxF 'AllowTcpForwarding yes' /etc/ssh/sshd_config || echo 'AllowTcpForwarding yes' >> /etc/ssh/sshd_config \
  && grep -qxF 'X11Forwarding yes' /etc/ssh/sshd_config || echo 'X11Forwarding yes' >> /etc/ssh/sshd_config

# g) Auto-source SGX SDK for interactive shells (bash & zsh)
RUN set -eux; \
  echo '[ -f /opt/sgxsdk/environment ] && . /opt/sgxsdk/environment' > /etc/profile.d/sgx-env.sh; \
  chmod 0644 /etc/profile.d/sgx-env.sh; \
  printf '%s\n' \
  'if [[ -o interactive && -f /opt/sgxsdk/environment ]]; then' \
  '  source /opt/sgxsdk/environment' \
  'fi' >> /etc/zsh/zshrc

# FINALLY - ensure user owns tools
RUN chown -R "${REMOTE_USER}:${REMOTE_GROUP}" /root/tools

EXPOSE 2222 6666
WORKDIR /workspaces/teaclave-storm
ENTRYPOINT ["/usr/local/bin/devcontainer-entrypoint"]
CMD ["/usr/sbin/sshd", "-D", "-e"]
