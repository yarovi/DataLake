# Run next command (inside root project)
python3 -m venv .venv
source .venv/bin/activate

# When you like exit 
desactive

# If you need continue this other day only run
source .venv/bin/activate

# To install dependency
pip install -r requirements.txt


# Import to run etl/main_v2.py as module
python -m etl.main_v2