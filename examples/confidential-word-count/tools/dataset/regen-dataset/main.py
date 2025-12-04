# regenerates the dataset by adding a random userId to each joke for user-level privacy testing
import sys
import os
import logging
import json
import random

# settings for userId assignment
NUM_USERS = 500
LOGNORMAL_MU = 0.0
LOGNORMAL_SIGMA = 1.0  # higher = heavier tail

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def main(dataset_path: str, output_path: str):
    logger.info(f"Loading dataset from '{dataset_path}'")

    # open file (read-only)
    with open(dataset_path, "r", encoding="utf-8") as f:
        dataset_data = json.load(f)

    if not isinstance(dataset_data, list):
        logger.error("Expected top-level JSON array of jokes.")
        sys.exit(1)

    logger.info(
        f"Assigning ~{NUM_USERS} userIds with lognormal skew (mu={LOGNORMAL_MU}, sigma={LOGNORMAL_SIGMA}) "
        f"to {len(dataset_data)} jokes"
    )

    users = list(range(1, NUM_USERS + 1))

    # we need to generate a long tailed distribution of user activity -> some users are more active then others
    # so, we generate lognormal weights for each user to be used for random sampling
    # SOURCES:
    # - https://stackoverflow.com/questions/39024785/long-tail-distribution-of-random-numbers-in-python
    # - https://numpy.org/doc/stable/reference/random/generated/numpy.random.lognormal.html -> not used, but has a nice plot
    weights = [random.lognormvariate(LOGNORMAL_MU, LOGNORMAL_SIGMA) for _ in users]

    # for each joke: validate + assign a user id for DP testing
    _required_keys = {"body", "category", "id", "rating"}
    for joke in dataset_data[:]:  # Iterate over a copy of the list
        if not isinstance(joke, dict):
            logger.error(f"Entry is not a JSON object, got: {type(joke)}")
            sys.exit(1)

        if not _required_keys.issubset(joke.keys()):
            logger.error(
                f"Entry is missing required keys. Expected keys: {_required_keys}, got: {joke.keys()}"
            )
            dataset_data.remove(joke)
            continue
        elif joke["body"] is None or joke["body"].strip() == "":
            logger.warning("Entry has empty body. Deleting entry.")
            dataset_data.remove(joke)
            continue

    # Reassign IDs after cleaning the list
    for i, joke in enumerate(dataset_data):
        joke["id"] = i

        # Weighted random choice to simulate long-tail user activity
        joke["user_id"] = random.choices(users, weights=weights, k=1)[0]

    # write updated dataset
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(dataset_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Written updated dataset with userIds to '{output_path}'")


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

    main(dataset_path, output_path)
