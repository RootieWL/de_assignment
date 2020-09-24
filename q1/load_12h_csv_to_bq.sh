# Create parition table
python3 load_csv_bq.py Sample-data-set brand_review_detail > brand_review_detail_$(date +"%F").log
# Load main table
python3 load_csv_bq.py Sample-data-set brand_review_audit > brand_review_audit_$(date +"%F").log
# Cron Job
# 0 */12 * * * python3 load_csv_bq.py Sample-data-set brand_review_detail > brand_review_detail_$(date +"%F").log
# 0 */12 * * * python3 load_csv_bq.py Sample-data-set brand_review_audit > brand_review_audit_$(date +"%F").log