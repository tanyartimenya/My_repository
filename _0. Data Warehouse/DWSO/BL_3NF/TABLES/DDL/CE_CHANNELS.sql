CREATE TABLE BL_3NF.CE_CHANNELS ( CHANNEL_ID NUMBER(38) NOT NULL, 
                            CHANNEL_SRC_ID VARCHAR(300) NOT NULL, 
                            SOURCE_SYSTEM VARCHAR(300) NOT NULL, 
                            SOURCE_TABLE VARCHAR(300) NOT NULL,
                            CHANNEL_NAME VARCHAR(300) NOT NULL,
                            TA_INSERT_DATE DATE NOT NULL, 
                            TA_UPDATE_DATE DATE NOT NULL);  

ALTER TABLE BL_3NF.CE_CHANNELS MODIFY TA_INSERT_DATE DEFAULT SYSDATE;
ALTER TABLE BL_3NF.CE_CHANNELS MODIFY TA_UPDATE_DATE DEFAULT SYSDATE;                          