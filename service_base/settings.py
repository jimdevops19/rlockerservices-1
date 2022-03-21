import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent

CLONED_REPOS_DIR = os.path.join(BASE_DIR, 'cloned_repos')

TOOLS_URL = "https://github.com/ebenahar/tools.git"
OCS_CI_URL = "https://github.com/red-hat-storage/ocs-ci.git"