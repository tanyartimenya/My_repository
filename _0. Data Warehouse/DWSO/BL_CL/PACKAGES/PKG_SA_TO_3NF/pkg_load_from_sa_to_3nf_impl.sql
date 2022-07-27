CREATE OR REPLACE PACKAGE BODY PKG_FULL_LOAD_TO_3NF AS

/*----------------------CE_COUNTRIES------------------------------------------ */
 PROCEDURE PRC_LOAD_CE_COUNTRIES IS 
BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_COUNTRIES';

INSERT INTO    BL_3NF.CE_COUNTRIES (COUNTRY_ID, COUNTRY_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, COUNTRY_NAME, TA_INSERT_DATE, TA_UPDATE_DATE) 
VALUES    (       - 1, - 1, 'MANUAL', 'MANUAL', 'N/A', SYSDATE, SYSDATE    );
COMMIT;
INSERT INTO
   BL_3NF.CE_COUNTRIES (COUNTRY_NAME, COUNTRY_SRC_ID, COUNTRY_ID, SOURCE_SYSTEM, SOURCE_TABLE, TA_INSERT_DATE, TA_UPDATE_DATE) 
   SELECT
      NVL(T.COUNTRY_NAME,'N/A')  AS COUNTRY_NAME,
      NVL(T.COUNTRY_SRC_ID, -1) AS COUNTRY_SRC_ID,
      COUNTRY_ID_SEQ.NEXTVAL AS COUNTRY_ID,
      'MANUAL' AS SOURCE_SYSTEM,
      'MANUAL' AS SOURCE_TABLE,
      SYSDATE AS TA_INSERT_DATE,
      SYSDATE AS TA_UPDATE_DATE 
   FROM
      (
         SELECT DISTINCT
            UPPER(COUNTRY) AS COUNTRY_NAME,
            UPPER(ISO_CODE_COUNTRY) AS COUNTRY_SRC_ID 
         FROM
            SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR 
         UNION
         SELECT DISTINCT
            UPPER(COUNTRY) AS COUNTRY_NAME,
            UPPER(ISO_CODE_COUNTRY) AS COUNTRY_SRC_ID 
         FROM
            sa_amocrm.src_get_quick_store
      )
      T;
      COMMIT;
COMMIT;
DBMS_OUTPUT.PUT_LINE ( 'TABLE WAS UPDATED SUCCESSFULLY.');
EXCEPTION 
WHEN
   OTHERS 
THEN
   DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
ROLLBACK;
END
PRC_LOAD_CE_COUNTRIES;


/*----------------------CE_CITIES------------------------------------------ */
 PROCEDURE PRC_LOAD_CE_CITIES IS 
BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_CITIES';
INSERT INTO
   BL_3NF.CE_CITIES (CITY_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, CITY_NAME, COUNTRY_ID, TA_INSERT_DATE, TA_UPDATE_DATE, CITY_ID) 
   SELECT
      NVL(T.CITY_SRC_ID,-1) AS CITY_SRC_ID,
      'MANUAL' AS SOURCE_SYSTEM,
      'MANUAL' AS SOURCE_TABLE,
      NVL(T.CITY_NAME,'N/A') AS CITY_NAME,
      NVL(COUNTRY_ID,-1) AS COUNTRY_ID,
      SYSDATE AS TA_INSERT_DATE,
      SYSDATE AS TA_UPDATE_DATE,
      CITY_ID_SEQ.NEXTVAL AS CITY_ID 
   FROM
      (
         SELECT DISTINCT
            CITY AS CITY_SRC_ID,
            CITY AS CITY_NAME,
            NVL(COUNTRY_ID,-1) AS COUNTRY_ID 
         FROM
            SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR 
            LEFT JOIN
               CE_COUNTRIES 
               ON SRC_EXPRESS_BAZAAR.ISO_CODE_COUNTRY = CE_COUNTRIES.COUNTRY_SRC_ID 
            UNION
            SELECT DISTINCT
               CITY AS CITY_SRC_ID,
               CITY AS CITY_NAME,
               COUNTRY_ID AS COUNTRY_ID 
            FROM
               sa_amocrm.src_get_quick_store 
               LEFT JOIN
                  CE_COUNTRIES 
                  ON src_get_quick_store.ISO_CODE_COUNTRY = CE_COUNTRIES.COUNTRY_SRC_ID
      )
      T;
INSERT INTO    BL_3NF.CE_CITIES (CITY_ID, CITY_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, CITY_NAME, COUNTRY_ID, TA_INSERT_DATE, TA_UPDATE_DATE) 
VALUES    (  - 1,       - 1,       'MANUAL',       'MANUAL',       'N/A',       - 1,       SYSDATE,       SYSDATE    );
COMMIT;
DBMS_OUTPUT.PUT_LINE ( 'TABLE WAS UPDATED SUCCESSFULLY.');
EXCEPTION 
WHEN
   OTHERS 
THEN
   DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
ROLLBACK;
END
PRC_LOAD_CE_CITIES;


/*----------------------CE_ADDRESSES------------------------------------------ */
 PROCEDURE PRC_LOAD_CE_ADDRESSES IS 
BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_ADDRESSES';
INSERT INTO
   BL_3NF.CE_ADDRESSES (ADDRESS_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, CITY_ID, ADDRESS_DESC, TA_INSERT_DATE, TA_UPDATE_DATE, ADDRESS_ID) 
   SELECT
      NVL(T.ADDRESS_SRC_ID,-1),
      NVL(T.SOURCE_SYSTEM,'MANUAL'),
      NVL(T.SOURCE_TABLE,'MANUAL'),
      NVL(T.CITY_ID,-1),
      NVL(T.ADDRESS_DESC,'N/A'),
      T.TA_INSERT_DATE,
      T.TA_UPDATE_DATE,
      ADDRESS_ID_SEQ.NEXTVAL 
   FROM
      (
         SELECT DISTINCT
            CUST_ID AS ADDRESS_SRC_ID,
            'RETAILCRM' AS SOURCE_SYSTEM,
            'ADDRESSES_SRC' AS SOURCE_TABLE,
            CITY_ID AS CITY_ID,
            ADDRESS_FULL AS ADDRESS_DESC,
            SYSDATE AS TA_INSERT_DATE,
            SYSDATE AS TA_UPDATE_DATE 
         FROM
            SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR 
            LEFT JOIN
               BL_3NF.CE_CITIES 
               ON SRC_EXPRESS_BAZAAR.CITY = CE_CITIES.CITY_NAME 
            UNION ALL
            SELECT DISTINCT
               CUST_ID AS ADDRESS_SRC_ID,
               'AMOCRM' AS SOURCE_SYSTEM,
               'ADDRESSES_SRC' AS SOURCE_TABLE,
               CITY_ID AS CITY_ID,
               ADDRESS_FULL AS ADDRESS_DESC,
               SYSDATE AS TA_INSERT_DATE,
               SYSDATE AS TA_UPDATE_DATE 
            FROM
               sa_amocrm.src_get_quick_store 
               LEFT JOIN
                  BL_3NF.CE_CITIES 
                  ON src_get_quick_store.CITY = CE_CITIES.CITY_NAME 
      )
      T;
INSERT INTO BL_3NF.CE_ADDRESSES (ADDRESS_ID, ADDRESS_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, CITY_ID, ADDRESS_DESC, TA_INSERT_DATE, TA_UPDATE_DATE) 
VALUES (- 1,  - 1, 'MANUAL', 'ADDRESSES_SRC', - 1, 'N/A', SYSDATE, SYSDATE );
COMMIT;
DBMS_OUTPUT.PUT_LINE ( 'TABLE WAS UPDATED SUCCESSFULLY.');
EXCEPTION 
WHEN
   OTHERS 
THEN
   DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
ROLLBACK;
END
PRC_LOAD_CE_ADDRESSES;


/*----------------------CE_STORES------------------------------------------ */
 PROCEDURE PRC_LOAD_CE_STORES
        IS BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_STORES';
            INSERT INTO BL_3NF.CE_STORES 
                                    (STORE_SRC_ID, 
                                    SOURCE_SYSTEM, 
                                    SOURCE_TABLE, 
                                    STORE_NAME, 
                                    TA_INSERT_DATE, 
                                    TA_UPDATE_DATE, 
                                    STORE_ID)
            SELECT  NVL(T.STORE_SRC_ID,-1), 
                    NVL(T.SOURCE_SYSTEM, 'N/A'),
                    NVL(T.SOURCE_TABLE, 'STORES_SRC'), 
                    NVL(T.STORE_NAME, 'N/A'), 
                    SYSDATE AS TA_INSERT_DATE, 
                    SYSDATE AS TA_UPDATE_DATE,
                    STORE_ID_SEQ.NEXTVAL
            FROM 
                    (SELECT DISTINCT STORE_ID AS STORE_SRC_ID, 
                    'RETAILCRM' AS SOURCE_SYSTEM, 
                    'STORES_SRC' AS SOURCE_TABLE, 
                     STORE_NAME AS STORE_NAME 
                                    FROM SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR 
            UNION 
                    SELECT DISTINCT STORE_ID AS STORE_SRC_ID, 
                    'AMOCRM' AS SOURCE_SYSTEM, 
                    'STORES_SRC' AS SOURCE_TABLE, 
                    STORE_NAME AS STORE_NAME
                                    FROM sa_amocrm.src_get_quick_store) T;
            INSERT INTO BL_3NF.CE_STORES (STORE_ID, STORE_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, STORE_NAME, TA_INSERT_DATE, TA_UPDATE_DATE)
            VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', SYSDATE, SYSDATE);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE (
                  'TABLE WAS UPDATED SUCCESSFULLY.');
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                ROLLBACK;
            END PRC_LOAD_CE_STORES; 


/*----------------------PKG_CE_PRODUCTS--------------------------------------- */
 PROCEDURE PRC_LOAD_CE_PRODUCTS
        IS BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_PRODUCTS';
                   INSERT INTO BL_3NF.CE_PRODUCTS 
                                (PRODUCT_SRC_ID, 
                                SOURCE_SYSTEM,  
                                SOURCE_TABLE, 
                                PRODUCT_NAME, 
                                TA_INSERT_DATE, 
                                TA_UPDATE_DATE, 
                                PRODUCT_ID)
                    SELECT NVL(T.PRODUCT_SRC_ID, -1), 
                                NVL(T.SOURCE_SYSTEM, 'MANUAL'), 
                                NVL(T.SOURCE_TABLE, 'MANUAL'), 
                                NVL(T.PRODUCT_NAME, 'N/A'), 
                                T.TA_INSERT_DATE, 
                                T.TA_UPDATE_DATE, 
                                PRODUCT_ID_SEQ.NEXTVAL
                        FROM 
                            (
                            SELECT DISTINCT ITEM_CODE AS PRODUCT_SRC_ID, 
                                'RETAILCRM' AS SOURCE_SYSTEM, 
                                'PRODUCTS_SRC' AS SOURCE_TABLE, 
                                ITEM_DESCRIPTION AS PRODUCT_NAME, 
                                SYSDATE AS TA_INSERT_DATE, 
                                SYSDATE AS TA_UPDATE_DATE
                            FROM SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR
                                                            UNION ALL
                            SELECT DISTINCT ITEM_CODE AS PRODUCT_SRC_ID, 
                                'AMOCRM' AS SOURCE_SYSTEM, 
                                'PRODUCTS_SRC' AS SOURCE_TABLE, 
                                ITEM_DESCRIPTION AS PRODUCT_NAME, 
                                SYSDATE AS TA_INSERT_DATE, 
                                SYSDATE AS TA_UPDATE_DATE
                            FROM sa_amocrm.src_get_quick_store
                            ) T;  
                    INSERT INTO BL_3NF.CE_PRODUCTS (PRODUCT_ID, PRODUCT_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, PRODUCT_NAME, TA_INSERT_DATE, TA_UPDATE_DATE)
                    VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', SYSDATE, SYSDATE);
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE (
                          'TABLE WAS UPDATED SUCCESSFULLY.');
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                        ROLLBACK;
            END PRC_LOAD_CE_PRODUCTS;


/*----------------------CE_EMPLOYEE_POSITIONS------------------------------------- */
 PROCEDURE PRC_LOAD_CE_EMPLOYEE_POSITIONS 
        IS BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_EMPLOYEE_POSITIONS';
    INSERT INTO BL_3NF.CE_EMPLOYEE_POSITIONS 
                            (EMPLOYEE_POSITION_SRC_ID, 
                            SOURCE_SYSTEM, 
                            SOURCE_TABLE, 
                            EMPLOYEE_POSITION_DESC, 
                            TA_INSERT_DATE, 
                            TA_UPDATE_DATE, 
                            EMPLOYEE_POSITION_ID)
    SELECT                  nvl(T.EMPLOYEE_POSITION_SRC_ID, -1),
                            nvl(T.SOURCE_SYSTEM, 'MANUAL'), 
                            nvl(T.SOURCE_TABLE, 'MANUAL'), 
                            nvl(T.EMPLOYEE_POSITION_DESC, 'MANUAL'), 
                            SYSDATE AS TA_INSERT_DATE, 
                            SYSDATE AS TA_UPDATE_DATE,
                            EMPLOYEE_POSITION_ID_SEQ.NEXTVAL
    FROM (
            SELECT DISTINCT EMPLOYEE_POSITION AS EMPLOYEE_POSITION_SRC_ID, 
                            'RETAILCRM' AS SOURCE_SYSTEM, 
                            'EMP_POSITIONS_SRC' AS SOURCE_TABLE, 
                            EMPLOYEE_POSITION AS EMPLOYEE_POSITION_DESC
                                FROM SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR
                                                            UNION ALL
            SELECT DISTINCT EMPLOYEE_POSITION AS EMPLOYEE_POSITION_SRC_ID, 
                            'AMOCRM' AS SOURCE_SYSTEM, 
                            'EMP_POSITIONS_SRC' AS SOURCE_TABLE, 
                            EMPLOYEE_POSITION AS EMPLOYEE_POSITION_DESC
                                FROM sa_amocrm.src_get_quick_store
                            ) T;
                    INSERT INTO BL_3NF.CE_EMPLOYEE_POSITIONS (EMPLOYEE_POSITION_ID, EMPLOYEE_POSITION_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, EMPLOYEE_POSITION_DESC, TA_INSERT_DATE, TA_UPDATE_DATE)
                    VALUES (-1, -1, 'MANUAL', 'EMP_POSITIONS_SRC', 'N/A', SYSDATE, SYSDATE);
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE (
                          'TABLE WAS UPDATED SUCCESSFULLY.');
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                        ROLLBACK;
    END PRC_LOAD_CE_EMPLOYEE_POSITIONS;    


----------------------PKG_CE_PAYMENT_TYPES--------------------------------- 


 PROCEDURE PRC_LOAD_CE_PAYMENT_TYPES 
        IS BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_PAYMENT_TYPES';
INSERT INTO BL_3NF.CE_PAYMENT_TYPES (PAYMENT_TYPE_SRC_ID, 
                            SOURCE_SYSTEM, 
                            SOURCE_TABLE, 
                            PAYMENT_TYPE_DESC, 
                            TA_INSERT_DATE, 
                            TA_UPDATE_DATE, 
                            PAYMENT_TYPE_ID)
                            SELECT nvl(T.PAYMENT_TYPE_SRC_ID,-1) AS PAYMENT_TYPE_SRC_ID,                
                            'MANUAL' AS SOURCE_SYSTEM, 
                            'MANUAL' AS SOURCE_TABLE,
                            nvl(T.PAYMENT_TYPE_DESC, 'N/A') AS PAYMENT_TYPE_DESC,
                            SYSDATE AS TA_INSERT_DATE, 
                            SYSDATE AS TA_UPDATE_DATE,
                            PAYMENT_TYPE_ID_SEQ.NEXTVAL AS PAYMENT_TYPE_ID
                            FROM (
                                    SELECT DISTINCT PAYMENT_TYPE AS PAYMENT_TYPE_SRC_ID,  
                                                    PAYMENT_TYPE AS PAYMENT_TYPE_DESC 
                                                    FROM SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR
                                                            UNION 
                                    SELECT DISTINCT PAYMENT_TYPE AS PAYMENT_TYPE_SRC_ID,  
                                    PAYMENT_TYPE AS PAYMENT_TYPE_DESC
                                    FROM SOURCE_GET_QUICK_STORE                             
                               ) T; 
                    INSERT INTO BL_3NF.CE_PAYMENT_TYPES (PAYMENT_TYPE_ID, PAYMENT_TYPE_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, PAYMENT_TYPE_DESC, TA_INSERT_DATE, TA_UPDATE_DATE)
                    VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', SYSDATE, SYSDATE);
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE (
                          'TABLE WAS UPDATED SUCCESSFULLY.');
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                        ROLLBACK;
    END PRC_LOAD_CE_PAYMENT_TYPES;    


----------------------PKG_CE_CHANNELS--------------------------------------
 PROCEDURE PRC_LOAD_CE_CHANNELS 
        IS BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_CHANNELS';
                INSERT INTO BL_3NF.CE_CHANNELS (CHANNEL_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, CHANNEL_NAME, TA_INSERT_DATE, TA_UPDATE_DATE, CHANNEL_ID)
                            SELECT nvl(T.CHANNEL_SRC_ID, -1) AS CHANNEL_SRC_ID,                
                            'MANUAL' AS SOURCE_SYSTEM, 
                            'MANUAL' AS SOURCE_TABLE,
                            nvl(T.CHANNEL_NAME, 'N/A') AS CHANNEL_NAME,
                            SYSDATE AS TA_INSERT_DATE, 
                            SYSDATE AS TA_UPDATE_DATE,
                            CHANNEL_ID_SEQ.NEXTVAL AS CHANNEL_ID
                            FROM (
                                    SELECT DISTINCT CHANNEL AS CHANNEL_SRC_ID,  
                                                    CHANNEL AS CHANNEL_NAME 
                                                    FROM SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR
                                                            UNION 
                                    SELECT DISTINCT CHANNEL AS CHANNEL_SRC_ID,  
                                    CHANNEL AS CHANNEL_NAME
                                    FROM SOURCE_GET_QUICK_STORE                             
                               ) T;  
                    INSERT INTO BL_3NF.CE_CHANNELS (CHANNEL_ID, CHANNEL_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, CHANNEL_NAME, TA_INSERT_DATE, TA_UPDATE_DATE)
                    VALUES (-1, -1, 'MANUAL', 'MANUAL', 'N/A', SYSDATE, SYSDATE);
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE (
                          'TABLE WAS UPDATED SUCCESSFULLY.');
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                        ROLLBACK;
            END PRC_LOAD_CE_CHANNELS; 


/*----------------------PKG_CE_CUSTOMERS_GEO--------------------------------- */
 PROCEDURE PRC_LOAD_CE_CUSTOMERS_GEO 
        IS BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_CUSTOMERS_GEO';
INSERT INTO BL_3NF.CE_CUSTOMERS_GEO (CUSTOMER_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, CITY_ID, ADDRESS_ID, TA_INSERT_DATE, TA_UPDATE_DATE, CUSTOMER_ID)
        SELECT NVL(T.CUSTOMER_SRC_ID, -1), T.SOURCE_SYSTEM, T.SOURCE_TABLE, NVL(T.CITY_ID, -1), NVL(T.ADDRESS_ID, -1), T.TA_INSERT_DATE, T.TA_UPDATE_DATE, CUSTOMERS_GEO_ID_SEQ.NEXTVAL
                        FROM 
                            (
                            SELECT DISTINCT CUST_ID AS CUSTOMER_SRC_ID, 'RETAILCRM' AS SOURCE_SYSTEM, 'CE_CUSTOMERS_GEO_SRC' AS SOURCE_TABLE, 
                            CITY_ID AS CITY_ID,
                            ADDRESS_ID AS ADDRESS_ID,
                            SYSDATE AS TA_INSERT_DATE, SYSDATE AS TA_UPDATE_DATE
                              FROM SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR
                                    LEFT JOIN CE_ADDRESSES
                                    ON SRC_EXPRESS_BAZAAR.ADDRESS_FULL = CE_ADDRESSES.ADDRESS_DESC
                                    AND CE_ADDRESSES.SOURCE_SYSTEM = 'RETAILCRM'
                                                            UNION ALL
                            SELECT DISTINCT CUST_ID AS CUSTOMER_SRC_ID, 'AMOCRM' AS SOURCE_SYSTEM, 'CE_CUSTOMERS_GEO_SRC' AS SOURCE_TABLE, 
                            CITY_ID AS CITY_ID,
                            ADDRESS_ID AS ADDRESS_ID,
                            SYSDATE AS TA_INSERT_DATE, SYSDATE AS TA_UPDATE_DATE
                            FROM SOURCE_GET_QUICK_STORE
                                    LEFT JOIN CE_ADDRESSES
                                    ON SOURCE_GET_QUICK_STORE.ADDRESS_FULL = CE_ADDRESSES.ADDRESS_DESC
                                    AND CE_ADDRESSES.SOURCE_SYSTEM = 'AMOCRM'
                            ) T;  
                            COMMIT;
                    DBMS_OUTPUT.PUT_LINE (
                          'TABLE WAS UPDATED SUCCESSFULLY.');
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                        ROLLBACK;
    END PRC_LOAD_CE_CUSTOMERS_GEO;


 PROCEDURE PRC_LOAD_CE_EMPLOYEES IS 
BEGIN
-------31.12.2019 is a start date of our business.------------------------------
INSERT INTO BL_3NF.CE_EMPLOYEES (EMPLOYEE_SRC_ID, SOURCE_SYSTEM, SOURCE_TABLE, LAST_NAME, FIRST_NAME, STORE_ID, EMPLOYEE_POSITION_ID, START_DATE, 
                        END_DATE, UPDATE_DATE, TA_INSERT_DATE, TA_UPDATE_DATE, EMPLOYEE_ID)
        SELECT NVL(T.EMPLOYEE_SRC_ID, -1), NVL(T.SOURCE_SYSTEM, 'N/A'), NVL(T.SOURCE_TABLE, 'N/A'), NVL(T.LAST_NAME, 'N/A'), NVL(T.FIRST_NAME, 'N/A'), NVL(T.STORE_ID, -1), NVL(T.EMPLOYEE_POSITION_ID, -1), 
        T.START_DATE , T.END_DATE, T.UPDATE_DATE, T.TA_INSERT_DATE, T.TA_UPDATE_DATE, EMPLOYEE_ID_SEQ.NEXTVAL
                        FROM 
                            (                            
                            SELECT distinct SRC.EMPLOYEE_ID AS EMPLOYEE_SRC_ID ,
                            'RETAILCRM' AS SOURCE_SYSTEM, 'EMPLOYEES_SRC' AS SOURCE_TABLE, 
                            SRC.EMPLOYEE_LAST_NAME AS LAST_NAME, SRC.EMPLOYEE_FIRST_NAME AS FIRST_NAME, NVL(STORE_ID,-1) AS STORE_ID, 
                            NVL(EMPLOYEE_POSITION_ID,-1) AS EMPLOYEE_POSITION_ID, 
                            TO_DATE(NVL(START_DATE, '31.12.2019')) AS START_DATE, 
                            TO_DATE(NVL(END_DATE, '31.12.9999')) AS END_DATE, 
                            sysdate AS UPDATE_DATE, 
                            SYSDATE AS TA_INSERT_DATE, SYSDATE AS TA_UPDATE_DATE
                            --NVL((SELECT EMPLOYEE_Id FROM BL_3NF.CE_EMPLOYEEST.EMPLOYEE_ID), EMPLOYEE_ID_SEQ.NEXTVAL) AS EMPLOYEE_Id
                            FROM SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR SRC
                             LEFT JOIN BL_3NF.CE_EMPLOYEE_POSITIONS
                                    ON SRC.EMPLOYEE_POSITION = CE_EMPLOYEE_POSITIONS.EMPLOYEE_POSITION_DESC
                                    AND CE_EMPLOYEE_POSITIONS.SOURCE_SYSTEM = 'RETAILCRM'
                                                            UNION 
                            SELECT distinct EMPLOYEE_ID AS EMPLOYEE_SRC_ID, 
                            'AMOCRM' AS SOURCE_SYSTEM, 'EMPLOYEES_SRC' AS SOURCE_TABLE, 
                            EMPLOYEE_LAST_NAME AS LAST_NAME, EMPLOYEE_FIRST_NAME AS FIRST_NAME,  NVL(STORE_ID,-1) AS STORE_ID, 
                            NVL(EMPLOYEE_POSITION_ID,-1) AS EMPLOYEE_POSITION_ID, 
                            NVL(START_DATE, '31.12.2019') AS START_DATE, 
                            NVL(END_DATE, '31.12.9999') AS END_DATE, 
                            sysdate AS UPDATE_DATE, 
                            SYSDATE AS TA_INSERT_DATE, SYSDATE AS TA_UPDATE_DATE
                            --NVL((SELECT EMPLOYEE_Id FROM BL_3NF.CE_EMPLOYEEST.EMPLOYEE_ID), EMPLOYEE_ID_SEQ.NEXTVAL) AS EMPLOYEE_Id
                            FROM sa_amocrm.src_get_quick_store
                             LEFT JOIN BL_3NF.CE_EMPLOYEE_POSITIONS
                                    ON src_get_quick_store.EMPLOYEE_POSITION = CE_EMPLOYEE_POSITIONS.EMPLOYEE_POSITION_DESC
                                    AND CE_EMPLOYEE_POSITIONS.SOURCE_SYSTEM = 'AMOCRM'
                            ) T
                     WHERE CONCAT(T.EMPLOYEE_SRC_ID,T.LAST_NAME) NOT IN (SELECT CONCAT(EMPLOYEE_SRC_ID, LAST_NAME) FROM BL_3NF.CE_EMPLOYEES); 
                     COMMIT;

DELETE FROM BL_CL.CLEAN_CE_EMPLOYEES_SCD2;
INSERT INTO BL_CL.CLEAN_CE_EMPLOYEES_SCD2   
        (
              SELECT DISTINCT TRG.EMPLOYEE_ID, SRC.EMPLOYEE_ID AS SRC_ID, 'AMOCRM' AS SOURCE_SYSTEM, SRC.EMPLOYEE_FIRST_NAME AS SRC_EMP_NAME, SRC.EMPLOYEE_LAST_NAME AS SRC_EMP_L_NAME,
                   TRG.EMPLOYEE_SRC_ID AS TRG_SRC_ID,
                   TRG.FIRST_NAME AS TRG_EMP_NAME, TRG.LAST_NAME AS TRG_EMP_L_NAME,
                    TRG.IS_CURRENT, TRG.START_DATE, TRG.END_DATE
                FROM      SA_AMOCRM.SRC_GET_QUICK_STORE SRC
                INNER JOIN BL_3NF.CE_EMPLOYEES TRG 
                ON SRC.EMPLOYEE_ID = TRG.EMPLOYEE_SRC_ID
                AND SRC.STORE_NAME = 'GetQuickStore'
                AND TRG.SOURCE_SYSTEM = 'AMOCRM'
                AND TRG.IS_CURRENT = 'Y'
                AND DECODE(SRC.EMPLOYEE_FIRST_NAME, TRG.FIRST_NAME,0,1)
                + DECODE(SRC.EMPLOYEE_LAST_NAME, TRG.LAST_NAME,0,1)>0
                UNION ALL
                 SELECT TRG.EMPLOYEE_ID, SRC.EMPLOYEE_ID AS SRC_ID, 'RETAILCRM' AS SOURCE_SYSTEM, SRC.EMPLOYEE_FIRST_NAME AS SRC_EMP_NAME, SRC.EMPLOYEE_LAST_NAME AS SRC_EMP_L_NAME,
                   TRG.EMPLOYEE_SRC_ID AS TRG_SRC_ID,
                   TRG.FIRST_NAME AS TRG_EMP_NAME, TRG.LAST_NAME AS TRG_EMP_L_NAME,
                    TRG.IS_CURRENT, TRG.START_DATE, TRG.END_DATE
                FROM      SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR SRC
                INNER JOIN BL_3NF.CE_EMPLOYEES TRG 
                ON SRC.EMPLOYEE_ID = TRG.EMPLOYEE_SRC_ID
                AND SRC.STORE_NAME = 'ExpressBazaar '
                AND TRG.SOURCE_SYSTEM = 'RETAILCRM'
                AND TRG.IS_CURRENT = 'Y'
                AND DECODE(SRC.EMPLOYEE_FIRST_NAME, TRG.FIRST_NAME,0,1)
                + DECODE(SRC.EMPLOYEE_LAST_NAME, TRG.LAST_NAME,0,1)>0); 
                COMMIT;

UPDATE BL_3NF.CE_EMPLOYEES E
        SET (E.EMPLOYEE_SRC_ID, E.END_DATE)
        = ( SELECT DISTINCT SRC_GET_QUICK_STORE.employee_id, MIN(SRC_GET_QUICK_STORE.end_date)
                                FROM SA_AMOCRM.SRC_GET_QUICK_STORE 
                                    left join ce_employees 
                                    on SRC_GET_QUICK_STORE.employee_id = ce_employees.employee_src_id and ce_employees.source_system = 'AMOCRM'
                                        where SRC_GET_QUICK_STORE.employee_last_name <> ce_employees.last_name and ce_employees.is_current = 'Y'
                                        GROUP BY SRC_GET_QUICK_STORE.employee_id
                                        ),
        E.IS_CURRENT = 'N',
        E.UPDATE_DATE = SYSDATE
        WHERE E.EMPLOYEE_SRC_ID IN (SELECT SRC_ID FROM BL_CL.CLEAN_CE_EMPLOYEES_SCD2)
        AND E.END_DATE <> TO_DATE('31.12.9999','DD.MM.YYYY')
        AND E.IS_CURRENT = 'Y';      
        COMMIT;
        
 DBMS_OUTPUT.PUT_LINE ( 'TABLE WAS UPDATED SUCCESSFULLY.');
EXCEPTION 
WHEN
   OTHERS 
THEN
   DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
ROLLBACK;
END PRC_LOAD_CE_EMPLOYEES;



/*----------------------CE_PAYMENTS------------------------------------------ */
 PROCEDURE PRC_LOAD_CE_PAYMENTS 
        IS BEGIN
         EXECUTE IMMEDIATE 'TRUNCATE TABLE BL_3NF.CE_PAYMENTS';       
INSERT /*+ append */  INTO BL_3NF.CE_PAYMENTS
    (       
            PAYMENT_SRC_ID,
            SOURCE_SYSTEM, 
            SOURCE_TABLE,
            PRODUCT_ID,
            CUSTOMER_ID,
            STORE_ID,
            EMPLOYEE_ID ,
            CHANNEL_ID ,
            PAYMENT_TYPE_ID,
            PAYMENT_DATE,
            AMOUNT ,
            QUANTITY ,
            TA_INSERT_DATE ,
            TA_UPDATE_DATE,
            PAYMENT_ID)  
SELECT T.*, 
BL_3NF.PAYMENT_ID_SEQ.NEXTVAL AS PAYMENT_ID
FROM (
SELECT              TRANSACTION_ID AS PAYMENT_SRC_ID, 
                    'RETAILCRM' AS SOURCE_SYSTEM, 'PAYMENTS_SRC' AS SOURCE_TABLE, 
                    NVL(CE_PRODUCTS.PRODUCT_ID, -1) AS PRODUCT_ID, 
                    NVL(CE_CUSTOMERS_GEO.CUSTOMER_ID, -1) AS CUSTOMER_ID, 
                    NVL(CE_STORES.STORE_ID, -1) AS STORE_ID, 
                    NVL(CE_EMPLOYEES.EMPLOYEE_ID, -1) AS EMPLOYEE_ID, 
                    NVL(CE_CHANNELS.CHANNEL_ID, -1) AS CHANNEL_ID, 
                    NVL(CE_PAYMENT_TYPES.PAYMENT_TYPE_ID, -1) AS PAYMENT_TYPE_ID,
                    TO_DATE(SRC.TRANSACTION_DATE,'DD.MM.YYYY') AS PAYMENT_DATE,
                    TO_NUMBER(SRC.COST_PER_ITEM, '9999999.99') AS AMOUNT, 
                    SRC.NUMBER_OF_ITEMS AS QUANTITY,
                    TO_DATE('01.01.2000','DD.MM.YYYY') AS TA_INSERT_DATE,
                    TO_DATE('01.01.2000','DD.MM.YYYY') AS TA_UPDATE_DATE
                            FROM SA_RETAIL_CRM.SRC_EXPRESS_BAZAAR SRC
                                    LEFT JOIN CE_EMPLOYEES
                                    ON SRC.EMPLOYEE_ID = CE_EMPLOYEES.EMPLOYEE_SRC_ID
                                    AND CE_EMPLOYEES.SOURCE_SYSTEM = 'RETAILCRM'
                                    AND CE_EMPLOYEES.SOURCE_TABLE = 'EMPLOYEES_SRC' 
                                    AND SRC.TRANSACTION_DATE >= CE_EMPLOYEES.start_date 
                                    AND SRC.TRANSACTION_DATE < CE_EMPLOYEES.end_date
                                LEFT JOIN CE_PAYMENT_TYPES
                                ON SRC.PAYMENT_TYPE = CE_PAYMENT_TYPES.PAYMENT_TYPE_SRC_ID
                                AND CE_PAYMENT_TYPES.SOURCE_SYSTEM = 'MANUAL'
                                AND CE_PAYMENT_TYPES.SOURCE_TABLE = 'MANUAL'
                                    LEFT JOIN CE_PRODUCTS
                                    ON SRC.ITEM_DESCRIPTION = CE_PRODUCTS.PRODUCT_NAME
                                    AND CE_PRODUCTS.SOURCE_SYSTEM = 'RETAILCRM'
                                    AND CE_PRODUCTS.SOURCE_TABLE = 'PRODUCTS_SRC'
                                        LEFT JOIN CE_CHANNELS
                                        ON SRC.CHANNEL = CE_CHANNELS.CHANNEL_NAME
                                        AND CE_CHANNELS.SOURCE_SYSTEM = 'MANUAL'
                                        AND CE_CHANNELS.SOURCE_TABLE = 'MANUAL'
                                            LEFT JOIN CE_CUSTOMERS_GEO
                                            ON SRC.CUST_ID = CE_CUSTOMERS_GEO.CUSTOMER_SRC_ID
                                            AND CE_CUSTOMERS_GEO.SOURCE_SYSTEM = 'RETAILCRM'
                                            AND CE_CUSTOMERS_GEO.SOURCE_TABLE = 'CE_CUSTOMERS_GEO_SRC'
                                        LEFT JOIN CE_STORES
                                        ON SRC.STORE_NAME = CE_STORES.STORE_NAME
                                        AND CE_STORES.SOURCE_SYSTEM = 'RETAILCRM'
                                        AND CE_STORES.SOURCE_TABLE = 'STORES_SRC'
 UNION ALL
 SELECT             
                    TRANSACTION_ID AS PAYMENT_SRC_ID, 
                    'AMOCRM' AS SOURCE_SYSTEM, 
                    'PAYMENTS_SRC' AS SOURCE_TABLE, 
                    NVL(CE_PRODUCTS.PRODUCT_ID, -1) AS PRODUCT_ID, 
                    NVL(CE_CUSTOMERS_GEO.CUSTOMER_ID, -1) AS CUSTOMER_ID, 
                    NVL(CE_STORES.STORE_ID, -1) AS STORE_ID, 
                    NVL(CE_EMPLOYEES.EMPLOYEE_ID, -1) AS EMPLOYEE_ID, 
                    NVL(CE_CHANNELS.CHANNEL_ID, -1) AS CHANNEL_ID, 
                    NVL(CE_PAYMENT_TYPES.PAYMENT_TYPE_ID, -1) AS PAYMENT_TYPE_ID,
                    TO_DATE(SRC.TRANSACTION_DATE,'DD.MM.YYYY') AS PAYMENT_DATE,
                    TO_NUMBER(SRC.COST_PER_ITEM, '9999999.99') AS AMOUNT, 
                    SRC.NUMBER_OF_ITEMS AS QUANTITY,
                    TO_DATE('01.01.2000','DD.MM.YYYY') AS TA_INSERT_DATE,
                    TO_DATE('01.01.2000','DD.MM.YYYY') AS TA_UPDATE_DATE
                            FROM SA_AMOCRM.SRC_GET_QUICK_STORE SRC
                                    LEFT JOIN CE_EMPLOYEES
                                    ON SRC.EMPLOYEE_ID = CE_EMPLOYEES.EMPLOYEE_SRC_ID
                                    AND CE_EMPLOYEES.SOURCE_SYSTEM = 'AMOCRM'
                                    AND CE_EMPLOYEES.SOURCE_TABLE = 'EMPLOYEES_SRC'
                                    AND SRC.TRANSACTION_DATE >= CE_EMPLOYEES.start_date 
                                    AND SRC.TRANSACTION_DATE < CE_EMPLOYEES.end_date
                                LEFT JOIN CE_PAYMENT_TYPES
                                ON SRC.PAYMENT_TYPE = CE_PAYMENT_TYPES.PAYMENT_TYPE_SRC_ID
                                AND CE_PAYMENT_TYPES.SOURCE_SYSTEM = 'MANUAL'
                                AND CE_PAYMENT_TYPES.SOURCE_TABLE = 'MANUAL'
                                    LEFT JOIN CE_PRODUCTS
                                    ON SRC.ITEM_DESCRIPTION = CE_PRODUCTS.PRODUCT_NAME
                                    AND CE_PRODUCTS.SOURCE_SYSTEM = 'AMOCRM'
                                    AND CE_PRODUCTS.SOURCE_TABLE = 'PRODUCTS_SRC'
                                        LEFT JOIN CE_CHANNELS
                                        ON SRC.CHANNEL = CE_CHANNELS.CHANNEL_NAME
                                        AND CE_CHANNELS.SOURCE_SYSTEM = 'MANUAL'
                                        AND CE_CHANNELS.SOURCE_TABLE = 'MANUAL'
                                            LEFT JOIN CE_CUSTOMERS_GEO
                                            ON SRC.CUST_ID = CE_CUSTOMERS_GEO.CUSTOMER_SRC_ID
                                            AND CE_CUSTOMERS_GEO.SOURCE_SYSTEM = 'AMOCRM'
                                            AND CE_CUSTOMERS_GEO.SOURCE_TABLE = 'CE_CUSTOMERS_GEO_SRC'
                                        LEFT JOIN CE_STORES
                                        ON SRC.STORE_NAME = CE_STORES.STORE_NAME
                                        AND CE_STORES.SOURCE_SYSTEM = 'AMOCRM'
                                        AND CE_STORES.SOURCE_TABLE = 'STORES_SRC') T;                                     
COMMIT; 

                    DBMS_OUTPUT.PUT_LINE (
                          'TABLE WAS UPDATED SUCCESSFULLY.');
                    EXCEPTION
                       WHEN OTHERS
                       THEN
                          DBMS_OUTPUT.PUT_LINE ('UPDATE TABLE FAILED.');
                        ROLLBACK;
END;    
END PKG_FULL_LOAD_TO_3NF;
                               
