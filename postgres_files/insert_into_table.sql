COPY clean_store_transactions(STORE_ID,STORE_LOCATION,PRODUCT_CATEGORY,PRODUCT_ID,MRP,CP,DISCOUNT,SP,Date) FROM '/tmp/clean_store_transactions.csv' DELIMITER ',' CSV HEADER;