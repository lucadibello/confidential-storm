# Requires: devcontainer CLI (npm i -g @devcontainers/cli) and Docker.
WORKSPACE       ?= ..
CONTAINER_NAME  ?= baseline-benchmark-devcontainer
SESSION         ?= dev

.PHONY: help devcontainer devcontainer-build devcontainer-up devcontainer-attach \
        devcontainer-attach-tmux devcontainer-down devcontainer-recreate tunnel-storm-ui

devcontainer: devcontainer-build devcontainer-up devcontainer-attach

devcontainer-build:
	devcontainer build --workspace-folder $(WORKSPACE)

devcontainer-up:
	devcontainer up --workspace-folder $(WORKSPACE)

devcontainer-attach: devcontainer-up
	devcontainer exec --workspace-folder $(WORKSPACE) bash

devcontainer-attach-tmux: devcontainer-up
	devcontainer exec --workspace-folder $(WORKSPACE) tmux new-session -A -s $(SESSION)

devcontainer-down:
	docker stop $(CONTAINER_NAME) || true
	docker rm -f -v $(CONTAINER_NAME) || true

devcontainer-recreate: devcontainer-down devcontainer-build devcontainer-up
