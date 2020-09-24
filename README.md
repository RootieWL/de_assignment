## de_assignment

For Q1
```
├── load_12h_csv_to_bq.sh      # 1. Crontab scheduler (every 12 hours)
├── load_csv_bq.py             # 2. Loads/cleans csv file, push to GCS, load GCS to BQ (audit/partition tables)          
└── params.py                  # 3. Load parameters to run GCP env
```
crontab env in GCE
```bash
0 */12 * * * python3 load_csv_bq.py Sample-data-set brand_review_detail > brand_review_detail_$(date +"%F").log
0 */12 * * * python3 load_csv_bq.py Sample-data-set brand_review_audit > brand_review_audit_$(date +"%F").log
```

For Q2
```   
└── q2_ans.sql                  # 1. Answers to question
```
