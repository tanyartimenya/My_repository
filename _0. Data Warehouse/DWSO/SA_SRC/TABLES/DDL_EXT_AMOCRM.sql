CREATE TABLE ext_amocrm.get_quick_store_ext
    (transaction_id VARCHAR2(300), transaction_date VARCHAR2(300),item_code VARCHAR2(300),
    item_description VARCHAR2(300), number_of_items VARCHAR2(300), cost_per_item VARCHAR2(300), cust_id VARCHAR2(300), 
    first_name VARCHAR2(300),last_name VARCHAR2(300), address_full VARCHAR2(300), address_short VARCHAR2(300), city VARCHAR2(300), 
    country VARCHAR2(300), iso_code_country VARCHAR2(300), payment_type VARCHAR2(300), channel VARCHAR2(300), store_id VARCHAR2(300),
    store_name VARCHAR2(300), employee_first_name VARCHAR2(300), employee_last_name VARCHAR2(300), employee_position VARCHAR2(300), employee_id VARCHAR2(300),
    start_date VARCHAR2(300), end_date VARCHAR2(300), update_date VARCHAR2(300), insert_date date
)
ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY external_tables
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        BADFILE 'badfile.log'
        SKIP 1
        FIELDS TERMINATED BY ','
        MISSING FIELD VALUES ARE NULL)
    LOCATION ( 'GetQuickStore5.csv' ))
    REJECT LIMIT UNLIMITED;
