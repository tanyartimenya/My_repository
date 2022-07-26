 -----------------------DIM-----------------------------------------------------
DROP SEQUENCE BL_DM.DIM_CHANNELS_ID_SEQ;
DROP SEQUENCE BL_DM.DIM_CUSTOMER_GEO_ID_SEQ;
DROP SEQUENCE BL_DM.DIM_EMPLOYEE_SCD_ID_SEQ;
DROP SEQUENCE BL_DM.DIM_PAYMENT_TYPES_ID_SEQ;
DROP SEQUENCE BL_DM.DIM_PRODUCTS_ID_SEQ;
DROP SEQUENCE BL_DM.FCT_PAYMENTS_ID_SEQ;
DROP SEQUENCE BL_DM.DIM_STORES_ID_SEQ;

CREATE SEQUENCE BL_DM.DIM_CHANNELS_ID_SEQ
 START WITH     1
 INCREMENT BY   1
 NOCACHE
 NOCYCLE;
 
 CREATE SEQUENCE BL_DM.DIM_CUSTOMER_GEO_ID_SEQ
 START WITH     1
 INCREMENT BY   1
 NOCACHE
 NOCYCLE;
 
 CREATE SEQUENCE BL_DM.DIM_EMPLOYEE_SCD_ID_SEQ
 START WITH     1
 INCREMENT BY   1
 NOCACHE
 NOCYCLE;
 
 CREATE SEQUENCE BL_DM.DIM_PAYMENT_TYPES_ID_SEQ
 START WITH     1
 INCREMENT BY   1
 NOCACHE
 NOCYCLE;
 
 CREATE SEQUENCE BL_DM.DIM_PRODUCTS_ID_SEQ
 START WITH     1
 INCREMENT BY   1
 NOCACHE
 NOCYCLE;
 
 CREATE SEQUENCE BL_DM.DIM_STORES_ID_SEQ
 START WITH     1
 INCREMENT BY   1
 NOCACHE
 NOCYCLE;
 
CREATE SEQUENCE BL_DM.FCT_PAYMENTS_ID_SEQ
 START WITH     1
 INCREMENT BY   1
 NOCACHE
 NOCYCLE;