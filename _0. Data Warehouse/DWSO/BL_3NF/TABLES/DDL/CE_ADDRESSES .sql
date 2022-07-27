CREATE TABLE BL_3NF.CE_ADDRESSES (
            ADDRESS_ID NUMBER(38) NOT NULL, 
            ADDRESS_SRC_ID VARCHAR2(300) NOT NULL,
            SOURCE_SYSTEM VARCHAR2(300) NOT NULL,
            SOURCE_TABLE VARCHAR2(300) NOT NULL,
            CITY_ID NUMBER(38) NOT NULL,
            ADDRESS_DESC VARCHAR2(300) NOT NULL,
            TA_INSERT_DATE DATE NOT NULL,
            TA_UPDATE_DATE DATE NOT NULL);

ALTER TABLE BL_3NF.CE_ADDRESSES MODIFY TA_INSERT_DATE DEFAULT SYSDATE;
ALTER TABLE BL_3NF.CE_ADDRESSES MODIFY TA_UPDATE_DATE DEFAULT SYSDATE;                            
