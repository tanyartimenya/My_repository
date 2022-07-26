CREATE TABLE BL_3NF.CE_EMPLOYEE_POSITIONS (
            EMPLOYEE_POSITION_ID NUMBER(38) NOT NULL, 
            EMPLOYEE_POSITION_SRC_ID VARCHAR2(300) NOT NULL,
            SOURCE_SYSTEM VARCHAR2(300) NOT NULL,
            SOURCE_TABLE VARCHAR2(300) NOT NULL,
            EMPLOYEE_POSITION_DESC VARCHAR2(300) NOT NULL,
            TA_UPDATE_DATE DATE NOT NULL,
            TA_INSERT_DATE DATE NOT NULL);                              

ALTER TABLE BL_3NF.CE_EMPLOYEE_POSITIONS MODIFY TA_INSERT_DATE DEFAULT SYSDATE;
ALTER TABLE BL_3NF.CE_EMPLOYEE_POSITIONS MODIFY TA_UPDATE_DATE DEFAULT SYSDATE;