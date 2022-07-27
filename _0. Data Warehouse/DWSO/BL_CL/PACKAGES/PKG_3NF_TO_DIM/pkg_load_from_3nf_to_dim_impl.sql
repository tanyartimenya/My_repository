/*---------------- DIM_STORES--------------------------------------- */
CREATE OR REPLACE PACKAGE BODY PKG_FULL_LOAD_DIM_TABLES AS 
    PROCEDURE PRC_LOAD_DIM_STORES
        IS
        BEGIN
---------------MERGE DIM_STORES------------------------------------------ 
                    MERGE INTO BL_DM.DIM_STORES NEW_T 
                    USING (SELECT           STORE_SRC_ID,
                                            'BL_3NF' AS SOURCE_SYSTEM, 
                                            'CE_STORES' AS SOURCE_TABLE, 
                                            STORE_NAME
                        FROM BL_3NF.CE_STORES
                        ) SRC
                        ON (NEW_T.STORE_SRC_ID  = SRC.STORE_SRC_ID
                        AND NEW_T.SOURCE_SYSTEM = SRC.SOURCE_SYSTEM
                        AND NEW_T.SOURCE_TABLE = SRC.SOURCE_TABLE)
                            WHEN MATCHED 
                                THEN UPDATE 
                                SET NEW_T.STORE_NAME = SRC.STORE_NAME,
                                    UPDATE_DT = SYSDATE
                                WHERE DECODE(NEW_T.STORE_NAME, 
                                            SRC.STORE_NAME, 0, 1)>0
                            WHEN NOT MATCHED 
                                THEN INSERT (STORE_SURR_ID,
                                            STORE_SRC_ID, 
                                            SOURCE_SYSTEM, 
                                            SOURCE_TABLE, 
                                            STORE_NAME, 
                                            INSERT_DT, 
                                            UPDATE_DT)
                                VALUES (BL_DM.DIM_STORES_ID_SEQ.NEXTVAL, 
                                            SRC.STORE_SRC_ID, 
                                            SRC.SOURCE_SYSTEM, 
                                            SRC.SOURCE_TABLE, 
                                            SRC.STORE_NAME, 
                                            SYSDATE, 
                                            SYSDATE);
                                            COMMIT;
            DBMS_OUTPUT.PUT_LINE (
                  'TABLE WAS UPDATED SUCCESSFULLY.');
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                ROLLBACK;
            END PRC_LOAD_DIM_STORES;


/*---------------- DIM_PRODUCTS------------------------------------- */
    PROCEDURE PRC_LOAD_DIM_PRODUCTS
        IS
        BEGIN
---------------MERGE DIM_PAYMENT_TYPES------------------------------------------ 
                    MERGE INTO BL_DM.DIM_PRODUCTS NEW_T 
                    USING (SELECT           PRODUCT_ID AS PRODUCT_SRC_ID,
                                            'BL_3NF' AS SOURCE_SYSTEM, 
                                            'CE_PRODUCTS' AS SOURCE_TABLE, 
                                            PRODUCT_NAME
                        FROM BL_3NF.CE_PRODUCTS
                        ) SRC
                        ON (NEW_T.PRODUCT_SRC_ID  = SRC.PRODUCT_SRC_ID
                        AND NEW_T.SOURCE_SYSTEM = SRC.SOURCE_SYSTEM
                        AND NEW_T.SOURCE_TABLE = SRC.SOURCE_TABLE)
                            WHEN MATCHED 
                                THEN UPDATE 
                                SET NEW_T.PRODUCT_NAME = SRC.PRODUCT_NAME,
                                    UPDATE_DT = SYSDATE
                                WHERE DECODE(NEW_T.PRODUCT_NAME, 
                                            SRC.PRODUCT_NAME, 0, 1)>0
                            WHEN NOT MATCHED 
                                THEN INSERT (PRODUCT_SURR_ID,
                                            PRODUCT_SRC_ID, 
                                            SOURCE_SYSTEM, 
                                            SOURCE_TABLE, 
                                            PRODUCT_NAME, 
                                            INSERT_DT, 
                                            UPDATE_DT)
                                VALUES (BL_DM.DIM_PRODUCTS_ID_SEQ.NEXTVAL, 
                                            SRC.PRODUCT_SRC_ID, 
                                            SRC.SOURCE_SYSTEM, 
                                            SRC.SOURCE_TABLE, 
                                            SRC.PRODUCT_NAME, 
                                            SYSDATE, 
                                            SYSDATE);
                                            COMMIT;
            DBMS_OUTPUT.PUT_LINE (
                  'TABLE WAS UPDATED SUCCESSFULLY.');
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                ROLLBACK;
            END PRC_LOAD_DIM_PRODUCTS;


/*---------------- DIM_CHANNELS------------------------------------- */
    PROCEDURE PRC_LOAD_DIM_CHANNELS
        IS
        BEGIN
---------------MERGE DIM_CHANNELS------------------------------------------ 
                    MERGE INTO BL_DM.DIM_CHANNELS NEW_T 
                    USING (SELECT           CHANNEL_ID ,
                                            'BL_3NF' AS SOURCE_SYSTEM, 
                                            'CE_CHANNELS' AS SOURCE_TABLE, 
                                            CHANNEL_NAME
                        FROM BL_3NF.CE_CHANNELS
                        ) SRC
                        ON (NEW_T.CHANNEL_SRC_ID  = SRC.CHANNEL_ID
                        AND NEW_T.SOURCE_SYSTEM = SRC.SOURCE_SYSTEM
                        AND NEW_T.SOURCE_TABLE = SRC.SOURCE_TABLE)
                            WHEN MATCHED 
                                THEN UPDATE 
                                SET NEW_T.CHANNEL_NAME = SRC.CHANNEL_NAME,
                                    UPDATE_DT = SYSDATE
                                WHERE DECODE(NEW_T.CHANNEL_NAME, 
                                            SRC.CHANNEL_NAME, 0, 1)>0
                            WHEN NOT MATCHED 
                                THEN INSERT (CHANNEL_SURR_ID,
                                            CHANNEL_SRC_ID, 
                                            SOURCE_SYSTEM, 
                                            SOURCE_TABLE, 
                                            CHANNEL_NAME, 
                                            INSERT_DT, 
                                            UPDATE_DT)
                                VALUES (BL_DM.DIM_CHANNELS_ID_SEQ.NEXTVAL, 
                                            SRC.CHANNEL_ID, 
                                            SRC.SOURCE_SYSTEM, 
                                            SRC.SOURCE_TABLE, 
                                            SRC.CHANNEL_NAME, 
                                            SYSDATE, 
                                            SYSDATE);
                                            COMMIT;
            DBMS_OUTPUT.PUT_LINE (
                  'TABLE WAS UPDATED SUCCESSFULLY.');
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                ROLLBACK;
            END PRC_LOAD_DIM_CHANNELS;
            


/*---------------- DIM_PAYMENT_TYPES-------------------------------- */
    PROCEDURE PRC_LOAD_DIM_PAYMENT_TYPE
        IS
        BEGIN
---------------MERGE DIM_PAYMENT_TYPES------------------------------------------
                    MERGE INTO BL_DM.DIM_PAYMENT_TYPES NEW_T 
                    USING (SELECT           PAYMENT_TYPE_ID,
                                            'BL_3NF' AS SOURCE_SYSTEM, 
                                            'CE_PAYMENT_TYPES' AS SOURCE_TABLE, 
                                            PAYMENT_TYPE_DESC
                        FROM BL_3NF.CE_PAYMENT_TYPES
                        ) SRC
                        ON (NEW_T.PAYMENT_TYPE_SRC_ID  = SRC.PAYMENT_TYPE_ID
                        AND NEW_T.SOURCE_SYSTEM = SRC.SOURCE_SYSTEM
                        AND NEW_T.SOURCE_TABLE = SRC.SOURCE_TABLE)
                            WHEN MATCHED 
                                THEN UPDATE 
                                SET NEW_T.PAYMENT_TYPE_DESC = SRC.PAYMENT_TYPE_DESC,
                                    UPDATE_DT = SYSDATE
                                WHERE DECODE(NEW_T.PAYMENT_TYPE_DESC, 
                                            SRC.PAYMENT_TYPE_DESC, 0, 1)>0
                            WHEN NOT MATCHED 
                                THEN INSERT (PAYMENT_TYPE_SURR_ID,
                                            PAYMENT_TYPE_SRC_ID, 
                                            SOURCE_SYSTEM, 
                                            SOURCE_TABLE, 
                                            PAYMENT_TYPE_DESC, 
                                            INSERT_DT, 
                                            UPDATE_DT)
                                VALUES (BL_DM.DIM_PAYMENT_TYPES_ID_SEQ.NEXTVAL, 
                                            SRC.PAYMENT_TYPE_ID, 
                                            SRC.SOURCE_SYSTEM, 
                                            SRC.SOURCE_TABLE, 
                                            SRC.PAYMENT_TYPE_DESC, 
                                            SYSDATE, 
                                            SYSDATE);
                                            COMMIT;
            DBMS_OUTPUT.PUT_LINE (
                  'TABLE WAS UPDATED SUCCESSFULLY.');
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                ROLLBACK;
            END PRC_LOAD_DIM_PAYMENT_TYPE;


/*----------------DIM_DATES------------------------------------- */
    PROCEDURE PRC_LOAD_DIM_DATES
        IS
        BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_DM.DIM_DATES';            
INSERT INTO BL_DM.DIM_DATES ( EVENT_DATE, 
                            DAY_NAME,
                            CALENDAR_MONTH_NAME ,
                            CALENDAR_QUARTER_NAME ,
                            CALENDAR_YEAR ,
                            DAY_NUMBER_IN_WEEK,
                            DAY_NUMBER_IN_MONTH ,
                            DAY_NUMBER_IN_YEAR ,
                            MONTH_NUMBER_IN_YEAR,
                            INSERT_DT ,
                            UPDATE_DATE)
                SELECT CURRENTDATE AS EVENT_DATE,
                TO_CHAR(CURRENTDATE,'YYYY') AS CALENDAR_YEAR,
                TO_CHAR(CURRENTDATE,'MONTH','NLS_DATE_LANGUAGE = ENGLISH') AS CALENDAR_MONTH_NAME,
                TO_CHAR(CURRENTDATE,'DAY','NLS_DATE_LANGUAGE = ENGLISH') AS DAY_NAME,
                TO_NUMBER(TRIM(LEADING '0' FROM TO_CHAR(CURRENTDATE,'D'))) AS DAY_NUMBER_IN_WEEK,
                TO_NUMBER(TRIM(LEADING '0' FROM TO_CHAR(CURRENTDATE,'DD'))) AS DAY_NUMBER_IN_MONTH,
                TO_NUMBER(TRIM(LEADING '0' FROM TO_CHAR(CURRENTDATE,'DDD'))) AS DAY_NUMBER_IN_YEAR,
                TO_NUMBER(TRIM(LEADING '0' FROM TO_CHAR(CURRENTDATE,'MM'))) AS MONTH_NUMBER_IN_YEAR,
                TO_NUMBER(TO_CHAR(CURRENTDATE,'Q')) AS CALENDAR_QUARTER_NAME,
                SYSDATE AS INSERT_DT,
                SYSDATE AS UPDATE_DATE
FROM (  SELECT LEVEL N,
        -- THE CALENDAR WILL BE FORMED STARTING FROM THE SPECIFIED DATE + 1 DAY.
       TO_DATE('31/12/1969','DD/MM/YYYY') + NUMTODSINTERVAL(LEVEL,'DAY') CURRENTDATE
        FROM DUAL
        -- HERE WE WRITE HOW MUCH DAYS WILL BE SHOWN IN OUR CALENDAR.
        CONNECT BY LEVEL <= 22000
        ORDER BY CURRENTDATE); 
COMMIT;
            DBMS_OUTPUT.PUT_LINE (
                  'TABLE WAS UPDATED SUCCESSFULLY.');
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                ROLLBACK;
            END PRC_LOAD_DIM_DATES;


/*---------------- DIM_CUSTOMERS_GEO-------------------------------- */ 
    PROCEDURE PRC_LOAD_DIM_CUSTOMERS_GEO 
        IS
        BEGIN
---------------MERGE DIM_CUSTOMERS_GEO------------------------------------------ 
                    MERGE INTO BL_DM.DIM_CUSTOMERS_GEO NEW_T 
                    USING (
                    SELECT CE_CUSTOMERS_GEO.CUSTOMER_ID, CE_COUNTRIES.COUNTRY_ID, 'BL_3NF' AS SOURCE_SYSTEM, 'CE_CUSTOMERS_GEO' AS SOURCE_TABLE, CE_COUNTRIES.COUNTRY_NAME, 
                            CE_CITIES.CITY_ID, CE_CITIES.CITY_NAME, CE_ADDRESSES.ADDRESS_ID AS ADDRESS_ID, CE_ADDRESSES.ADDRESS_DESC AS ADDRESS
                        FROM BL_3NF.CE_CUSTOMERS_GEO LEFT JOIN BL_3NF.CE_ADDRESSES
                        ON BL_3NF.CE_ADDRESSES.ADDRESS_ID = BL_3NF.CE_CUSTOMERS_GEO.ADDRESS_ID
                            LEFT JOIN BL_3NF.CE_CITIES
                            ON BL_3NF.CE_CITIES.CITY_ID = BL_3NF.CE_CUSTOMERS_GEO.CITY_ID
                                LEFT JOIN BL_3NF.CE_COUNTRIES
                                ON BL_3NF.CE_COUNTRIES.COUNTRY_ID = BL_3NF.CE_CITIES.COUNTRY_ID
                        ) SRC
                        ON (NEW_T.ADDRESS_ID = SRC.ADDRESS_ID
                        )
                            WHEN MATCHED 
                                THEN UPDATE 
                                SET NEW_T.COUNTRY_NAME = SRC.COUNTRY_NAME,
                                    NEW_T.COUNTRY_ID = SRC.COUNTRY_ID,
                                    NEW_T.CITY_NAME = SRC.CITY_NAME,
                                    NEW_T.CITY_ID = SRC.CITY_ID, 
                                    NEW_T.ADDRESS = SRC.ADDRESS,
                                    UPDATE_DT = SYSDATE
                                WHERE DECODE(NEW_T.COUNTRY_NAME, SRC.COUNTRY_NAME, 0, 1)
                                      + DECODE(NEW_T.CITY_NAME, SRC.CITY_NAME, 0, 1)
                                      + DECODE(NEW_T.ADDRESS, SRC.ADDRESS, 0, 1)
                                      + DECODE(NEW_T.COUNTRY_ID , SRC.COUNTRY_ID, 0, 1)
                                      + DECODE(NEW_T.CITY_ID, SRC.CITY_ID, 0, 1) >0
                            WHEN NOT MATCHED 
                                THEN INSERT (CUSTOMER_SURR_ID,
                                            CUSTOMER_SRC_ID,
                                            COUNTRY_ID, 
                                            SOURCE_SYSTEM, 
                                            SOURCE_TABLE, 
                                            COUNTRY_NAME,
                                            CITY_ID,
                                            CITY_NAME,
                                            ADDRESS_ID,
                                            ADDRESS,
                                            INSERT_DT, 
                                            UPDATE_DT)
                                VALUES (BL_DM.DIM_CUSTOMER_GEO_ID_SEQ.NEXTVAL,
                                            SRC.CUSTOMER_ID,
                                            SRC.COUNTRY_ID, 
                                            SRC.SOURCE_SYSTEM, 
                                            SRC.SOURCE_TABLE, 
                                            SRC.COUNTRY_NAME, 
                                            SRC.CITY_ID, 
                                            SRC.CITY_NAME,
                                            SRC.ADDRESS_ID,
                                            SRC.ADDRESS,
                                            SYSDATE, 
                                            SYSDATE);
                                            COMMIT;
            DBMS_OUTPUT.PUT_LINE (
                  'TABLE WAS UPDATED SUCCESSFULLY.');
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                  ROLLBACK;
            END PRC_LOAD_DIM_CUSTOMERS_GEO;


/*---------------- DIM_EMPLOYEES_SCD-------------------------------- */
 PROCEDURE PRC_LOAD_DIM_EMPLOYEES_SCD
        IS
        BEGIN
---------------MERGE DIM_EMPLOYEES_SCD------------------------------------------ 
                    MERGE INTO BL_DM.DIM_EMPLOYEES_SCD NEW_T 
                    USING (
                    SELECT EMPLOYEE_ID AS EMPLOYEE_SRC_ID, 
                            'BL_3NF' AS SOURCE_SYSTEM, 'CE_EMPLOYEES' AS SOURCE_TABLE, LAST_NAME AS EMPLOYEE_LAST_NAME, FIRST_NAME AS EMPLOYEE_FIRST_NAME,
                            EMPLOYEE_POSITION_DESC AS EMPLOYEE_POSITION, IS_CURRENT, START_DATE, END_DATE
                    FROM BL_3NF.CE_EMPLOYEES EMP
                        LEFT JOIN BL_3NF.CE_EMPLOYEE_POSITIONS POS
                        ON EMP.EMPLOYEE_POSITION_ID = POS.EMPLOYEE_POSITION_ID
                        ) SRC
                        ON (NEW_T.EMPLOYEE_SRC_ID = SRC.EMPLOYEE_SRC_ID
                        AND NEW_T.SOURCE_SYSTEM = SRC.SOURCE_SYSTEM
                        AND NEW_T.SOURCE_TABLE = SRC.SOURCE_TABLE
                        AND NEW_T.START_DATE = SRC.START_DATE
                        )
                            WHEN MATCHED 
                                THEN UPDATE 
                                SET NEW_T.EMPLOYEE_LAST_NAME = SRC.EMPLOYEE_LAST_NAME,
                                    NEW_T.EMPLOYEE_FIRST_NAME = SRC.EMPLOYEE_FIRST_NAME,
                                    NEW_T.EMPLOYEE_POSITION = SRC.EMPLOYEE_POSITION,
                                    NEW_T.IS_CURRENT = SRC.IS_CURRENT,
                                    NEW_T.END_DATE = END_DATE,
                                    UPDATE_DT = SYSDATE
                                WHERE DECODE(NEW_T.EMPLOYEE_LAST_NAME, SRC.EMPLOYEE_LAST_NAME, 0, 1)
                                      + DECODE(NEW_T.EMPLOYEE_FIRST_NAME, SRC.EMPLOYEE_FIRST_NAME, 0, 1)
                                      + DECODE(NEW_T.EMPLOYEE_POSITION, SRC.EMPLOYEE_POSITION, 0, 1)
                                      + DECODE(NEW_T.IS_CURRENT, SRC.IS_CURRENT, 0, 1)
                                      + DECODE(NEW_T.START_DATE, SRC.START_DATE, 0, 1)
                                      + DECODE(NEW_T.END_DATE , SRC.END_DATE, 0, 1) > 0
                            WHEN NOT MATCHED 
                                THEN INSERT (
                                            EMPLOYEE_SURR_ID, 
                                            EMPLOYEE_SRC_ID,
                                            SOURCE_SYSTEM ,
                                            SOURCE_TABLE ,
                                            EMPLOYEE_LAST_NAME ,
                                            EMPLOYEE_FIRST_NAME ,
                                            EMPLOYEE_POSITION ,
                                            IS_CURRENT ,
                                            START_DATE,
                                            END_DATE,
                                            INSERT_DT,
                                            UPDATE_DT)
                                VALUES (BL_DM.DIM_EMPLOYEE_SCD_ID_SEQ.NEXTVAL,
                                            SRC.EMPLOYEE_SRC_ID,
                                            SRC.SOURCE_SYSTEM ,
                                            SRC.SOURCE_TABLE ,
                                            SRC.EMPLOYEE_LAST_NAME ,
                                            SRC.EMPLOYEE_FIRST_NAME ,
                                            SRC.EMPLOYEE_POSITION ,
                                            SRC.IS_CURRENT ,
                                            TO_DATE(SRC.START_DATE, 'DD.MM.YYYY'),
                                            TO_DATE(SRC.END_DATE, 'DD.MM.YYYY'),
                                            SYSDATE,
                                            SYSDATE);
                                            COMMIT;
            DBMS_OUTPUT.PUT_LINE (
                  'TABLE WAS UPDATED SUCCESSFULLY.');
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                  ROLLBACK;
            END PRC_LOAD_DIM_EMPLOYEES_SCD;


/*----------------------FCT_PAYMENTS------------------------------------------ */
   PROCEDURE PRC_LOAD_FCT_PAYMENTS
        IS BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_DM.FCT_PAYMENTS';
INSERT /*+ append */  INTO BL_DM.FCT_PAYMENTS 
    (       
            EVENT_DATE,
            PAYMENT_ID,
            PRODUCT_SURR_ID  ,
            CUSTOMER_SURR_ID,
            STORE_SURR_ID ,
            EMPLOYEE_SURR_ID, 
            CHANNEL_SURR_ID ,
            PAYMENT_TYPE_SURR_ID,
            QUANTITY ,
            AMOUNT,
            INSERT_DT
             )  
            SELECT T.*
            FROM (
            SELECT  TO_DATE(PAY.PAYMENT_DATE, 'DD.MM.YYYY') AS EVENT_DATE,
                    PAY.PAYMENT_ID,
                    NVL(DIM_PRODUCTS.PRODUCT_SURR_ID,-1) AS PRODUCT_SURR_ID  ,
                    NVL(DIM_CUSTOMERS_GEO.CUSTOMER_SURR_ID,-1) AS CUSTOMER_SURR_ID,
                    NVL(DIM_STORES.STORE_SURR_ID,-1) AS STORE_SURR_ID ,
                    NVL(DIM_EMPLOYEES_SCD.EMPLOYEE_SURR_ID,-1) AS EMPLOYEE_SURR_ID ,
                    NVL(DIM_CHANNELS.CHANNEL_SURR_ID,-1) AS  CHANNEL_SURR_ID ,
                    NVL(DIM_PAYMENT_TYPES.PAYMENT_TYPE_SURR_ID,-1) AS PAYMENT_TYPE_SURR_ID,
                    PAY.QUANTITY,
                    PAY.AMOUNT,
                    SYSDATE AS INSERT_DT
            FROM BL_3NF.CE_PAYMENTS PAY
                LEFT JOIN BL_DM.DIM_STORES 
                ON DIM_STORES.STORE_SRC_ID = PAY.STORE_ID
                AND DIM_STORES.SOURCE_SYSTEM = 'BL_3NF'
                AND DIM_STORES.SOURCE_TABLE = 'CE_STORES'
                    LEFT JOIN BL_DM.DIM_CHANNELS
                    ON DIM_CHANNELS.CHANNEL_SRC_ID = PAY.CHANNEL_ID
                    AND DIM_CHANNELS.SOURCE_SYSTEM = 'BL_3NF'
                    AND DIM_CHANNELS.SOURCE_TABLE = 'CE_CHANNELS'
                        LEFT JOIN BL_DM.DIM_PRODUCTS
                        ON DIM_PRODUCTS.PRODUCT_SRC_ID = PAY.PRODUCT_ID
                        AND DIM_PRODUCTS.SOURCE_SYSTEM = 'BL_3NF'
                        AND DIM_PRODUCTS.SOURCE_TABLE = 'CE_PRODUCTS' 
                            LEFT JOIN BL_DM.DIM_CUSTOMERS_GEO
                            ON DIM_CUSTOMERS_GEO.CUSTOMER_SRC_ID = PAY.CUSTOMER_ID
                            AND DIM_CUSTOMERS_GEO.SOURCE_SYSTEM = 'BL_3NF'
                            AND DIM_CUSTOMERS_GEO.SOURCE_TABLE = 'CE_CUSTOMERS_GEO'
                                LEFT JOIN BL_DM.DIM_PAYMENT_TYPES
                                ON DIM_PAYMENT_TYPES.PAYMENT_TYPE_SRC_ID = PAY.PAYMENT_TYPE_ID
                                AND DIM_PAYMENT_TYPES.SOURCE_SYSTEM = 'BL_3NF'
                                AND DIM_PAYMENT_TYPES.SOURCE_TABLE = 'CE_PAYMENT_TYPES'
                                    LEFT JOIN BL_DM.DIM_EMPLOYEES_SCD 
                                    ON PAY.EMPLOYEE_ID = BL_DM.DIM_EMPLOYEES_SCD.EMPLOYEE_SRC_ID
                                    AND BL_DM.DIM_EMPLOYEES_SCD.SOURCE_SYSTEM = 'BL_3NF'
                                    AND BL_DM.DIM_EMPLOYEES_SCD.SOURCE_TABLE = 'CE_EMPLOYEES'
                                    AND TO_DATE(PAY.PAYMENT_DATE, 'DD.MM.YYYY') >= TO_DATE(BL_DM.DIM_EMPLOYEES_SCD.START_DATE , 'DD.MM.YYYY')
                                    AND TO_DATE(PAY.PAYMENT_DATE, 'DD.MM.YYYY') < TO_DATE(BL_DM.DIM_EMPLOYEES_SCD.END_DATE, 'DD.MM.YYYY')
            ) T
        WHERE CONCAT(T.PAYMENT_ID, T.EVENT_DATE) 
                NOT IN (SELECT CONCAT(FCT_PAYMENTS.PAYMENT_ID,FCT_PAYMENTS.EVENT_DATE) FROM BL_DM.FCT_PAYMENTS);                                      
COMMIT; 
                    DBMS_OUTPUT.PUT_LINE (
                          'TABLE WAS UPDATED SUCCESSFULLY.');
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                        ROLLBACK;
    END PRC_LOAD_FCT_PAYMENTS;    
END PKG_FULL_LOAD_DIM_TABLES;