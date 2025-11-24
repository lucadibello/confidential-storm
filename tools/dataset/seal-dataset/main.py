import sys
import os
import logging
import json
import base64

# needed for encryption + data integrity
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.hashes import Hash, SHA256


# Setup logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

# constants
SOURCE_NAME = "_DATASET" # unique name to identify source
DESTINATION_NAME = "_MAPPER" # special component inside topology that will map the AAD to the real source/destinations


def load_stream_key() -> bytes:
    """
    Load a fixed symmetric key from env var STREAM_KEY_HEX (64 hex chars -> 32 bytes).
    """
    key_hex = os.environ.get("STREAM_KEY_HEX")
    if not key_hex:
        logger.error(
            "Missing STREAM_KEY_HEX env var. "
            "Set it to a 64-char hex string (32 bytes) for ChaCha20-Poly1305."
        )
        sys.exit(1)

    try:
        key = bytes.fromhex(key_hex)
    except ValueError:
        logger.error("STREAM_KEY_HEX is not valid hex.")
        sys.exit(1)

    if len(key) != 32:
        logger.error(
            f"STREAM_KEY_HEX must be 32 bytes (64 hex chars), got {len(key)} bytes."
        )
        sys.exit(1)

    return key


def main(dataset_path: str, output_path: str):
    # Load fixed key once
    key = load_stream_key()
    aead = ChaCha20Poly1305(key)

    # parse json file -> prepare each tuple
    with open(dataset_path, "r", encoding="utf-8") as f:
        # load json data (if crash -> not valid json file)
        dataset_data = json.load(f)

    if not isinstance(dataset_data, list):
        logger.error("Expected dataset to be a JSON array (list of entries).")
        sys.exit(1)

    encrypted_entries = []

    # Read each entry and prepare encryption
    for i, entry in enumerate(dataset_data):
        # serialize entire entry as json (plaintext)
        entry_json = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))

        # random nonce (12 bytes) – included in ciphertext package
        nonce = os.urandom(12)

        # build header (this will later be AAD)
        # NOTE: this is empty at generation, but may be recomputed during
        header = {
            "source": SOURCE_NAME,
            "destination": DESTINATION_NAME,
        }

        # serialize header as JSON
        header = json.dumps(header, sort_keys=True, separators=(',', ':'))

        # ChaCha20-Poly1305: ct includes ciphertext + tag (aad)
        ct = aead.encrypt(nonce, entry_json.encode("utf-8"), header.encode("utf-8"))

        # log every 1000 entries
        if i % 1000 == 0:
            # log json representation for comparison
            logger.info(
                f"Processed entry {i}: "
                f"header={header}, "
                f"plaintext_len={len(entry_json)}, "
                f"ciphertext_len={len(ct)}"
            )


        # store encoded values
        encrypted_entry = {
            "header": header,  # AAD-reconstructable on the other side
            "nonce": base64.b64encode(nonce).decode("ascii"),
            "ciphertext": base64.b64encode(ct).decode("ascii"),
        }
        encrypted_entries.append(encrypted_entry)

    # Write encrypted dataset as JSON
    with open(output_path, "w", encoding="utf-8") as out_f:
        json.dump(encrypted_entries, out_f, ensure_ascii=False, indent=2)

    logger.info(
        f"Encrypted dataset written to '{output_path}' "
        f"({len(encrypted_entries)} entries)."
    )


if __name__ == "__main__":
    # ensure correct number of arguments
    if len(sys.argv) != 3:
        logger.error("Usage: python prepare-dataset.py <dataset_path> <output_path>")
        sys.exit(1)

    dataset_path = sys.argv[1]
    output_path = sys.argv[2]

    # Ensure arguments are strings
    if not isinstance(dataset_path, str):
        logger.error("dataset_path must be a string")
        sys.exit(1)
    if not isinstance(output_path, str):
        logger.error("output_path must be a string")
        sys.exit(1)

    # Ensure that source dataset exists
    if not os.path.exists(dataset_path):
        logger.error(f"dataset_path '{dataset_path}' does not exist")
        sys.exit(1)

    # Ensure the parent directory for output exists
    parent_dir = os.path.dirname(output_path) or "."
    os.makedirs(parent_dir, exist_ok=True)

    # Seal source dataset -> produce encrypted dataset
    main(dataset_path, output_path)
