CREATE TABLE BL_DM.FCT_PAYMENTS (
            EVENT_DATE  DATE,  
            PAYMENT_ID NUMBER(38),
            PRODUCT_SURR_ID  NUMBER(38) NOT NULL,
            CUSTOMER_SURR_ID NUMBER(38) NOT NULL,
            STORE_SURR_ID NUMBER(38) NOT NULL,
            EMPLOYEE_SURR_ID NUMBER(38) NOT NULL,
            CHANNEL_SURR_ID NUMBER(38) NOT NULL,
            PAYMENT_TYPE_SURR_ID NUMBER(38) NOT NULL,
            QUANTITY NUMBER(10, 2),
            AMOUNT NUMBER(10, 2),
            INSERT_DT DATE);
