PYTHON_BIN ?= python3

REGEN_PY = regen-dataset/main.py
SEAL_PY = seal-dataset/main.py

SOURCE ?= jokes.json
REGEN_PREFIX ?= regen
SEAL_PREFIX ?= enc
DESTINATION_DIR ?= out
STREAM_KEY_FILE ?= stream_key_hex

SOURCE_NAME := $(notdir $(SOURCE))
REGEN_DATASET := $(DESTINATION_DIR)/$(REGEN_PREFIX)-$(SOURCE_NAME)
SEALED_DATASET := $(DESTINATION_DIR)/$(SEAL_PREFIX)-$(SOURCE_NAME)

ifdef STREAM_KEY_HEX
ENCRYPT_KEY_DEPS :=
ENCRYPT_ENV :=
else
ENCRYPT_KEY_DEPS := $(STREAM_KEY_FILE)
ENCRYPT_ENV := STREAM_KEY_HEX=$$(cat $(STREAM_KEY_FILE)) 
endif

.PHONY: all regenerate encrypt key clean

all: regenerate encrypt

$(DESTINATION_DIR):
	mkdir -p $@

$(REGEN_DATASET): $(SOURCE) | $(DESTINATION_DIR)
	$(PYTHON_BIN) $(REGEN_PY) $< $@

$(SEALED_DATASET): $(REGEN_DATASET) $(ENCRYPT_KEY_DEPS) | $(DESTINATION_DIR)
	$(ENCRYPT_ENV)$(PYTHON_BIN) $(SEAL_PY) $< $@

$(STREAM_KEY_FILE):
	@mkdir -p $(@D)
	@printf '%s\n' \
		"import pathlib" \
		"import secrets" \
		"import sys" \
		"" \
		"path = pathlib.Path(sys.argv[1])" \
		"if path.exists():" \
		"    print(f\"Stream key already exists at {path}\")" \
		"    sys.exit(0)" \
		"" \
		"key = secrets.token_hex(32)" \
		"path.write_text(key + \"\\n\", encoding=\"utf-8\")" \
		"print(f\"Generated new stream key at {path}\")" \
	| $(PYTHON_BIN) - "$@"

regenerate: $(REGEN_DATASET)

encrypt: $(SEALED_DATASET)

key: $(STREAM_KEY_FILE)

clean:
	rm -rf $(DESTINATION_DIR)
