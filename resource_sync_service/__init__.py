from pathlib import Path
import datetime
import yaml
import os


package_dir = Path(__file__).resolve().parent
conf_location = os.path.join(package_dir, "defaultconf.yaml")

with open(conf_location) as f:
    conf = yaml.safe_load(f)
