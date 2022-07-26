CREATE TABLE BL_DM.DIM_DATES (
            EVENT_DATE DATE NOT NULL,
            DAY_NAME VARCHAR2(300),
            CALENDAR_MONTH_NAME VARCHAR2(300),
            CALENDAR_QUARTER_NAME VARCHAR2(300),
            CALENDAR_YEAR VARCHAR2(300),
            DAY_NUMBER_IN_WEEK NUMBER(38),
            DAY_NUMBER_IN_MONTH NUMBER(38),
            DAY_NUMBER_IN_YEAR NUMBER(38),
            MONTH_NUMBER_IN_YEAR NUMBER(38),
            INSERT_DT DATE,
            UPDATE_DATE DATE);