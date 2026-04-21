import sys
import os
import logging
import json
import base64
import argparse

# needed for encryption + data integrity
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305, AESGCM


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

# Supported encryption schemes -- must match EncryptionScheme.java enum names
SUPPORTED_SCHEMES = ["CHACHA20_POLY1305", "AES_256_GCM", "NONE"]
DEFAULT_SCHEME = "CHACHA20_POLY1305"


def make_aead(scheme: str, key: bytes):
    """
    Create an AEAD cipher instance for the given scheme, or None for NONE.
    """
    if scheme == "CHACHA20_POLY1305":
        return ChaCha20Poly1305(key)
    elif scheme == "AES_256_GCM":
        return AESGCM(key)
    elif scheme == "NONE":
        return None
    else:
        raise ValueError(f"Unknown encryption scheme: {scheme}")


def encrypt_value(aead, scheme: str, plaintext: bytes, header: str) -> dict:
    """
    Encrypt (or pass through) a value using the selected scheme.
    Returns a dict with header, nonce (base64), and ciphertext (base64).
    """
    header_bytes = header.encode("utf-8")

    if scheme == "NONE":
        # Passthrough: zero nonce, plaintext stored as-is
        nonce = bytes(12)
        ct = plaintext
    else:
        nonce = os.urandom(12)
        ct = aead.encrypt(nonce, plaintext, header_bytes)

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
            "Set it to a 64-char hex string (32 bytes)."
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


def main(dataset_path: str, output_path: str, scheme: str):
    # Load fixed key once
    key = load_stream_key()
    aead = make_aead(scheme, key)

    logger.info(f"Using encryption scheme: {scheme}")
    if scheme == "NONE":
        logger.warning(
            "NONE scheme selected -- dataset will NOT be encrypted. "
            "Use only for benchmarking."
        )

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
        encrypted_user_id = encrypt_value(
            aead, scheme, user_id_json.encode("utf-8"), header
        )

        # Encrypt payload (body, category, id, rating - without user_id)
        encrypted_payload = encrypt_value(
            aead, scheme, payload_json.encode("utf-8"), header
        )

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
        f"Sealed dataset written to '{output_path}' "
        f"({len(encrypted_entries)} entries, scheme={scheme})."
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Seal (encrypt) a jokes dataset for Confidential Storm."
    )
    parser.add_argument("dataset_path", help="Path to the input JSON dataset")
    parser.add_argument("output_path", help="Path to write the sealed output JSON")
    parser.add_argument(
        "--scheme",
        choices=SUPPORTED_SCHEMES,
        default=DEFAULT_SCHEME,
        help=(
            f"Encryption scheme (default: {DEFAULT_SCHEME}). "
            "Must match the EncryptionScheme configured in the enclave."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Ensure that source dataset exists
    if not os.path.exists(args.dataset_path):
        logger.error(f"dataset_path '{args.dataset_path}' does not exist")
        sys.exit(1)

    # Ensure the parent directory for output exists
    parent_dir = os.path.dirname(args.output_path) or "."
    os.makedirs(parent_dir, exist_ok=True)

    # Seal source dataset -> produce encrypted dataset
    main(args.dataset_path, args.output_path, args.scheme)
