import sys
import os

# Add your project directory to Python's path
project_home = '/var/www/flask_app'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

# Import from the Flask subdirectory
from Flask.HTML import app as application
