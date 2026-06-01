# create venv using venv module
python3 -m venv .venv

# install required packages from requirements.txt
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# create data directory if it doesn't exist
mkdir -p data

# initialize ipykernel from the venv
python -m ipykernel install --user --name confidentialstorm --display-name "Python (confidentialstorm)"

