import sys
import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append('..')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(BASE_DIR, 'credentials/gcp_credentials.json')
