CREATE TABLE BL_DM.DIM_PAYMENT_TYPES (
            PAYMENT_TYPE_SURR_ID NUMBER(38), 
            PAYMENT_TYPE_SRC_ID NUMBER(38),
            SOURCE_SYSTEM VARCHAR2(300),
            SOURCE_TABLE VARCHAR2(300),
            PAYMENT_TYPE_DESC VARCHAR2(300),
            INSERT_DT DATE,
            UPDATE_DT  DATE);
