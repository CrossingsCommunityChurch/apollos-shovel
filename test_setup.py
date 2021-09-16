import sys
import os
import time

os.environ["TZ"] = "UTC"
time.tzset()

# Add the dags to the python module lookup tree.
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/dags")
