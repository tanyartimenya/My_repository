--------------------CREATE LOGGING TABLE---------------------------------------
CREATE TABLE BL_CL.PRC_LOG_T (L_INSERT_DATE TIMESTAMP,
                              L_DB_INSTANCE VARCHAR2(200),
                              L_SESSION_ID VARCHAR2(200),
                              L_ROW_COUNT NUMBER,
                              L_DATE DATE,
                              L_LOGIN_USER VARCHAR2(200),
                              L_OS_USER VARCHAR2(200),
                              L_TERMINAL VARCHAR2(200),
                              L_CALL_STACK VARCHAR2(2000)
                              );
                              
                              
-------------------CREATE LOGGING PROCEDURE-------------------------------------                             
CREATE OR REPLACE NONEDITIONABLE PROCEDURE PRC_LOG 
        (L_INSERTED_ROWS IN NUMBER
        )
        
AS PRAGMA AUTONOMOUS_TRANSACTION;
        L_INSERT_DATE   TIMESTAMP := SYSDATE;
        L_SESSION_ID    VARCHAR2(200);
        L_DATE          DATE := SYSDATE;
        L_LOGIN_USER    VARCHAR2(200);
        L_OS_USER       VARCHAR2(200);
        L_TERMINAL      VARCHAR2(200);
        L_DB_INSTANCE   VARCHAR2(200);
        L_CALL_STACK    VARCHAR2(2000);
BEGIN
        L_SESSION_ID :=  DBMS_SESSION.UNIQUE_SESSION_ID;
        L_LOGIN_USER :=  TO_CHAR(SYS_CONTEXT('USERENV', 'Session_user'));
        L_DB_INSTANCE := TO_CHAR(SYS_CONTEXT('USERENV', 'DB_NAME'));
        L_OS_USER     := TO_CHAR(SYS_CONTEXT('USERENV', 'OS_USER'));
        L_TERMINAL    := TO_CHAR(SYS_CONTEXT('USERENV', 'TERMINAL'));
        L_CALL_STACK  := DBMS_UTILITY.FORMAT_CALL_STACK;
        INSERT INTO BL_CL.PRC_LOG_T (L_INSERT_DATE,  
        L_DB_INSTANCE, L_SESSION_ID, L_ROW_COUNT, L_DATE, L_LOGIN_USER, L_OS_USER, L_CALL_STACK, L_TERMINAL)
        SELECT L_INSERT_DATE, L_DB_INSTANCE, L_SESSION_ID, L_INSERTED_ROWS, L_DATE, L_LOGIN_USER, L_OS_USER, L_CALL_STACK, L_TERMINAL
        FROM DUAL;
COMMIT;
END PRC_LOG;

--------------------------------------------------------------------------------  
EXEC BL_CL.PRC_LOG;
                              
DROP TABLE  BL_CL.PRC_LOG_T;
SELECT * FROM BL_CL.PRC_LOG_T;