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

# constants - these must match ComponentConstants.java in the topology
SOURCE_NAME = "_DATASET"  # ComponentConstants.DATASET
DESTINATION_NAME = "random-joke-spout"  # ComponentConstants.RANDOM_JOKE_SPOUT


def encrypt_value(aead: ChaCha20Poly1305, plaintext: bytes, header: str) -> dict:
    """
    Encrypt a value using ChaCha20-Poly1305 AEAD.
    Returns a dict with header, nonce (base64), and ciphertext (base64).
    """
    nonce = os.urandom(12)
    ct = aead.encrypt(nonce, plaintext, header.encode("utf-8"))
    return {
        "header": header,
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ct).decode("ascii"),
    }


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

    # build header (this will later be AAD) - same for all entries
    # NOTE: header contains routing metadata for the topology
    header = json.dumps(
        {"source": SOURCE_NAME, "destination": DESTINATION_NAME},
        sort_keys=True,
        separators=(",", ":"),
    )

    # Read each entry and prepare encryption
    for i, entry in enumerate(dataset_data):
        # ensure we have a user id
        if "user_id" not in entry:
            raise ValueError(f"Entry {i} missing required 'user_id' field.")

        # Extract user_id for separate encryption
        user_id = entry["user_id"]

        # Create payload without user_id (user_id will be encrypted separately)
        payload = {k: v for k, v in entry.items() if k != "user_id"}

        # Serialize payloads
        user_id_json = json.dumps({"user_id": user_id}, separators=(",", ":"))
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

        # Encrypt user_id separately (smaller blob, decrypt only when needed for DP)
        encrypted_user_id = encrypt_value(aead, user_id_json.encode("utf-8"), header)

        # Encrypt payload (body, category, id, rating - without user_id)
        encrypted_payload = encrypt_value(aead, payload_json.encode("utf-8"), header)

        # log every 1000 entries
        if i % 1000 == 0:
            logger.info(
                f"Processed entry {i}: "
                f"user_id_len={len(user_id_json)}, "
                f"payload_len={len(payload_json)}"
            )

        # store as dual-encrypted entry
        encrypted_entry = {
            "userId": encrypted_user_id,  # separately encrypted user_id
            "payload": encrypted_payload,  # encrypted payload (body, category, id, rating)
        }
        encrypted_entries.append(encrypted_entry)

    # Write encrypted dataset as JSON
    with open(output_path, "w", encoding="utf-8") as out_f:
        json.dump(encrypted_entries, out_f, ensure_ascii=False, indent=2)

    logger.info(
        f"Encrypted dataset written to '{output_path}' "
        f"({len(encrypted_entries)} entries with separate user_id encryption)."
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
