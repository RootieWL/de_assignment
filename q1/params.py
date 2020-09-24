import os
import sys
from datetime import date
from datetime import datetime

# insert google credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './sample_key.json'

# insert parameters here
date = datetime.now().strftime("%Y-%m-%d_%Hh-%Mm-%Ss") 
project = 'sample_project'
bucket = 'sample_bucket'
location = 'asia-souteast1' #singapore

tgt_dataset = 'sample_dataset'

# schedules to main/partition table 
csv_f = sys.argv[1]
tgt_tbl = sys.argv[2]
