CREATE TABLE BL_DM.DIM_CUSTOMERS_GEO (
            CUSTOMER_SURR_ID NUMBER(38), 
            CUSTOMER_SRC_ID NUMBER(38),
            SOURCE_SYSTEM VARCHAR2(300),
            SOURCE_TABLE VARCHAR2(300),
            COUNTRY_NAME VARCHAR2(300),
            COUNTRY_ID NUMBER(38),
            CITY_ID  NUMBER(38),
            CITY_NAME VARCHAR2(300),
            ADDRESS VARCHAR2(300),
            ADDRESS_ID  NUMBER(38),
            INSERT_DT DATE,
            UPDATE_DT DATE);