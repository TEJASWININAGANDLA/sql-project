

CREATE TABLE EMP_RAW.EMPLOYEE (
    ESSN VARCHAR(9) PRIMARY KEY,
    FNAME VARCHAR(50),
    MINIT CHAR(1),
    LNAME VARCHAR(50),
    BDATE DATE,
    ADDRESS VARCHAR(100),
    SEX CHAR(1),
    SALARY DECIMAL(10, 2),
    SUPER_SSN VARCHAR(9),
    DNO INT
);

CREATE TABLE EMP_RAW.DEPARTMENT (
    DNUMBER INT PRIMARY KEY,
    DNAME VARCHAR(50),
    MGR_SSN VARCHAR(9),
    MGR_START_DATE DATE
);

CREATE TABLE EMP_RAW.DEPT_LOCATIONS (
    DNUMBER INT,
    DLOCATION VARCHAR(50),
    PRIMARY KEY (DNUMBER, DLOCATION)
);

CREATE TABLE EMP_RAW.PROJECT (
    PNUMBER INT PRIMARY KEY,
    PNAME VARCHAR(50),
    PLOCATION VARCHAR(50),
    DNUM INT
);

CREATE TABLE EMP_RAW.WORKS_ON (
    ESSN VARCHAR(9),
    PNO INT,
    HOURS DECIMAL(4, 1),
    PRIMARY KEY (ESSN, PNO)
);

CREATE TABLE EMP_RAW.DEPENDENT (
    ESSN VARCHAR(9),
    DEPENDENT_NAME VARCHAR(50),
    SEX CHAR(1),
    BDATE DATE,
    RELATIONSHIP VARCHAR(25),
    PRIMARY KEY (ESSN, DEPENDENT_NAME)
);



CREATE TABLE EMP_PROC.EMP_SAL_GREATER_MNGR (
    ESSN VARCHAR(9),
    SALARY DECIMAL(10, 2),
    SUPER_SSN VARCHAR(9),
    SUPER_SALARY DECIMAL(10, 2)
);

CREATE TABLE EMP_PROC.EMP_PROJECT_DEPT (
    ESSN VARCHAR(9),
    PNAME VARCHAR(50),
    EMP_DEPT_NAME VARCHAR(50),
    PROJ_DEPT_NAME VARCHAR(50)
);

CREATE TABLE EMP_PROC.EMP_DEPT_LEAST (
    DEPT_NAME VARCHAR(50),
    DEPT_NO INT,
    NO_OF_EMP INT
);

CREATE TABLE EMP_PROC.EMP_TOT_HRS_SPENT (
    ESSN VARCHAR(9),
    DEPENDENT_NAME VARCHAR(50),
    PNO INT,
    TOTAL_HOURS_SPENT DECIMAL(10, 2)
);

CREATE TABLE EMP_PROC.EMP_FULL_DETAILS (
    ESSN VARCHAR(9),
    FNAME VARCHAR(50),
    MINIT CHAR(1),
    LNAME VARCHAR(50),
    BDATE DATE,
    ADDRESS VARCHAR(100),
    SEX CHAR(1),
    SALARY DECIMAL(10, 2),
    SUPER_SSN VARCHAR(9),
    DNO INT,
    DNAME VARCHAR(50),
    DLOCATION VARCHAR(50),
    PNAME VARCHAR(50),
    PNUMBER INT,
    PLOCATION VARCHAR(50),
    TOTAL_HOURS DECIMAL(10, 2),
    DEPENDENT_NAME VARCHAR(50),
    DEPENDENT_SEX CHAR(1),
    DEPENDENT_RELATION VARCHAR(25)
);
