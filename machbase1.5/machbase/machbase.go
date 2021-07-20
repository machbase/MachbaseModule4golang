package machbase

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <machbase_sqlcli.h>

#define RC_SUCCESS          0
#define RC_FAILURE          -1
#define ERROR_CHECK_COUNT   100

//******************************************************* pointer array create
static char* makeErrChar(int aMaxColSize)
{
    return malloc(sizeof(char) * aMaxColSize);
}

static char** makeAppendCharArray(int aLength)
{
    return calloc(aLength, sizeof(char*));
}

static int* makeAppendTypeArray(int aLength)
{
    return calloc(aLength, sizeof(int));
}

static char** makeColNameArray(int aColumnCount, int aColumnSize)
{
    char** sArr = calloc(aColumnCount, sizeof(char*));
    int i = 0;

    for(i = 0; i < aColumnCount; i++)
    {
        sArr[i] = (char *)malloc(sizeof(char) * aColumnSize);
    }

    return sArr;
}

static SQLSMALLINT* makeColTypeArray(int aColumnCount)
{
    return calloc(aColumnCount, sizeof(SQLSMALLINT));
}

static SQLULEN* makeColLengthArray(int aColumnCount)
{
    return calloc(aColumnCount, sizeof(SQLULEN));
}

static SQLLEN* makeColRLengthArray(int aColumnCount)
{
    return calloc(aColumnCount, sizeof(SQLLEN));
}

static long long* makeLongArray(int aColumnCount)
{
    return calloc(aColumnCount, sizeof(long long));
}

static double* makeDoubleArray(int aColumnCount)
{
    return calloc(aColumnCount, sizeof(double));
}

static char** makeBindCharArray(int aColumnCount, int aMaxColSize, int aBlobColSize, short* aColumnTypeArr)
{
    char** sArr = calloc(aColumnCount, sizeof(char*));
    int i = 0;

    for(i = 0; i < aColumnCount; i++)
    {
        if (aColumnTypeArr[i] == -2 || aColumnTypeArr[i] == 2100)
        {
            sArr[i] = (char *)malloc(sizeof(char) * aBlobColSize);
        }
        else
        {
            sArr[i] = (char *)malloc(sizeof(char) * aMaxColSize);
        }
    }

    return sArr;
}

//******************************************************* pointer array data setting
static void setArrayString(char **aCharArr, char *aChar, int aIndex)
{
    aCharArr[aIndex] = aChar;
}

static void setArrayInt(int *aIntArr, int aValue, int aIndex)
{
    aIntArr[aIndex] = aValue;
}

static void setArrayLong(long long *aLongArr, long long aValue, int aIndex)
{
    aLongArr[aIndex] = aValue;
}

static void setArrayDouble(double *aDoubleArr, double aValue, int aIndex)
{
    aDoubleArr[aIndex] = aValue;
}

static char* setNilString()
{
    char* sResult = malloc(sizeof(char) * 2);
    sResult[0] = 0;

    return sResult;
}

//******************************************************* pointer array free
static void freeCharArray(char **aCharArr, int aColumnCount)
{
    int i;
    for (i = 0; i < aColumnCount; i++)
        free(aCharArr[i]);
    free(aCharArr);
}

static void freeChar(char *aChar)
{
    free(aChar);
}

static void freeAppendTypeArray(int *aAppendTypeArr)
{
    free(aAppendTypeArr);
}

static void freeColTypeArray(SQLSMALLINT *aColumnTypeArr)
{
    free(aColumnTypeArr);
}

static void freeColLenArray(SQLULEN *aColumnLengArr)
{
    free(aColumnLengArr);
}

static void freeColRLenArray(SQLLEN *aColumnRLengArr)
{
    free(aColumnRLengArr);
}

static void freeLongArray(long long *aArr)
{
    free(aArr);
}

static void freeDoubleArray(double *aArr)
{
    free(aArr);
}

//******************************************************* Get data from pointer array start
static char* getCharValue(char** aCharArr, int aIndex)
{
    return aCharArr[aIndex];
}

static SQLLEN getColLen(SQLLEN* aColumnLengArr, int aIndex)
{
    return aColumnLengArr[aIndex];
}

static int getShortValue(SQLSMALLINT* aIntArr, int aIndex)
{
    return (int)aIntArr[aIndex];
}

static long getlongValue(SQLULEN* aLongArr, int aIndex)
{
    return (long)aLongArr[aIndex];
}

static long long getlonglongValue(long long* aLongLongArr, int aIndex)
{
    return (long long)aLongLongArr[aIndex];
}

static double getDoubleValue(double* adoubleArr, int aIndex)
{
    return (double)adoubleArr[aIndex];
}

//******************************************************* machbase c code start
void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStr, char *aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS )
    {
        sprintf(aErrStr, "%s\nSQLSTATE-[%s], Machbase-[%d][%s]", aMsg, sSqlState, sNativeError, sErrorMsg);
    }
    else
    {
        sprintf(aErrStr, "%s\n", aMsg);
    }
}

int connectDB(SQLHENV *aEnv, SQLHDBC *aCon, char *aCConErr, char *aDriver)
{
    if( SQLAllocEnv(aEnv) != SQL_SUCCESS )
    {
        strcpy(aCConErr, "SQLAllocEnv error");

        return RC_FAILURE;
    }

    if( SQLAllocConnect(*aEnv, aCon) != SQL_SUCCESS )
    {
        strcpy(aCConErr, "SQLAllocConnect error");

        SQLFreeEnv(*aEnv);
        *aEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    if( SQLDriverConnect( *aCon, NULL,
                          (SQLCHAR *)aDriver,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {
        printError(*aEnv, *aCon, NULL, aCConErr, "SQLDriverConnect error");

        SQLFreeConnect(*aCon);
        *aCon = SQL_NULL_HDBC;

        SQLFreeEnv(*aEnv);
        *aEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

int disconnectDB(SQLHENV *aEnv, SQLHDBC *aCon)
{
    if( SQLDisconnect(*aCon) != SQL_SUCCESS )
    {
        return RC_FAILURE;
    }

    SQLFreeConnect(*aCon);
    *aCon = SQL_NULL_HDBC;

    SQLFreeEnv(*aEnv);
    *aEnv = SQL_NULL_HENV;

    return RC_SUCCESS;
}

int allocStmt(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT *aStmt, char *aCStmtErr)
{
    *aStmt = SQL_NULL_HSTMT;

    if( SQLAllocStmt(aCon, aStmt) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, *aStmt, aCStmtErr, "SQLAllocStmt Error");

        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int execDirect(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr, char *aSQL)
{
    if( SQLExecDirect(aStmt, (SQLCHAR *)aSQL, SQL_NTS) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLExecDirect Error");

        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int freeStmt(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT *aStmt)
{
    if( SQLFreeStmt(*aStmt, SQL_DROP) != SQL_SUCCESS )
    {
        return RC_FAILURE;
    }

    *aStmt = SQL_NULL_HSTMT;
    return RC_SUCCESS;
}

int appendOpen(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr, char *aTableName)
{
    if( SQLAppendOpen(aStmt, (SQLCHAR *)aTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLAppendOpen Error");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int appendDataV2(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr, int *aTypeArr, char **aValueArr, char *aDateFormat, int aLength, int aByteArrSize)
{
    int i;
    SQL_APPEND_PARAM *sParam = malloc(sizeof(SQL_APPEND_PARAM) * aLength);
    memset(sParam,0,sizeof(SQL_APPEND_PARAM) * aLength);
    for(i=0; i < aLength; i++)
    {
        if (aValueArr[i][0] == '\0')
        {
            switch(aTypeArr[i])
            {
                case 11:
                    sParam[i].mUShort = SQL_APPEND_USHORT_NULL;
                    break;
                case 0:
                    sParam[i].mShort = (short)SQL_APPEND_SHORT_NULL;
                    break;
                case 12:
                    sParam[i].mUInteger = SQL_APPEND_UINTEGER_NULL;
                    break;
                case 1:
                    sParam[i].mInteger = (int)SQL_APPEND_INTEGER_NULL;
                    break;
                case 13:
                    sParam[i].mULong = SQL_APPEND_ULONG_NULL;
                    break;
                case 2:
                    sParam[i].mLong = (long long)SQL_APPEND_LONG_NULL;
                    break;
                case 3:
                    sParam[i].mFloat = SQL_APPEND_FLOAT_NULL;
                    break;
                case 4:
                    sParam[i].mDouble = SQL_APPEND_DOUBLE_NULL;
                    break;
                case 8:
                    sParam[i].mDateTime.mTime = (long long)SQL_APPEND_DATETIME_NULL;
                    sParam[i].mDateTime.mFormatStr = aDateFormat;
                    sParam[i].mDateTime.mDateStr = aValueArr[i];
                    break;
                case 10:
                case 9:
                case 5:
                    sParam[i].mVar.mData = aValueArr[i];
                    sParam[i].mVar.mLength = SQL_APPEND_TEXT_NULL;
                    break;
                case 6:
                case 7:
                    sParam[i].mIP.mLength = SQL_APPEND_IP_NULL;
                    sParam[i].mIP.mAddrString = aValueArr[i];
                    break;
                default:
                    break;
            }
        }
        else
        {
            switch(aTypeArr[i])
            {
                case 11:
                    sParam[i].mUShort = atoi(aValueArr[i]);
                    break;
                case 0:
                    sParam[i].mShort = atoi(aValueArr[i]);
                    break;
                case 12:
                    sParam[i].mUInteger = atoi(aValueArr[i]);
                    break;
                case 1:
                    sParam[i].mInteger = atoi(aValueArr[i]);
                    break;
                case 13:
                    sParam[i].mULong = atoll(aValueArr[i]);
                    break;
                case 2:
                    sParam[i].mLong = atoll(aValueArr[i]);
                    break;
                case 3:
                    sParam[i].mFloat = atof(aValueArr[i]);
                    break;
                case 4:
                    sParam[i].mDouble = atof(aValueArr[i]);
                    break;
                case 8:
                    if(aDateFormat[0] == '\0')
                    {
                        sParam[i].mDateTime.mTime = atoll(aValueArr[i]);
                    }
                    else
                    {
                        sParam[i].mDateTime.mTime = SQL_APPEND_DATETIME_STRING;
                        sParam[i].mDateTime.mFormatStr = aDateFormat;
                        sParam[i].mDateTime.mDateStr = aValueArr[i];
                    }
                    break;
                case 10:
                    sParam[i].mVar.mData = aValueArr[i];
                    sParam[i].mVar.mLength = aByteArrSize;
                    break;
                case 9:
                case 5:
                    sParam[i].mVar.mData = aValueArr[i];
                    sParam[i].mVar.mLength = strlen(aValueArr[i]);
                    break;
                case 6:
                case 7:
                    sParam[i].mIP.mLength = SQL_APPEND_IP_STRING;
                    sParam[i].mIP.mAddrString = aValueArr[i];
                    break;
                default:
                    break;
            }
        }
    }

    if( SQLAppendDataV2(aStmt, sParam) != SQL_SUCCESS )
    {
        free(sParam);
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLAppendData Error");
        return RC_FAILURE;
    }
    else
    {
        free(sParam);
        return RC_SUCCESS;
    }
}

int appendDataV2I(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr, int *aTypeArr, long long *aLongValueArr, double *aDoubleValueArr, char **aStringValueArr, char *aDateFormat, int aLength, int aByteArrSize)
{
    int i;
    SQL_APPEND_PARAM *sParam = malloc(sizeof(SQL_APPEND_PARAM) * aLength);
    memset(sParam,0,sizeof(SQL_APPEND_PARAM) * aLength);
    for(i=0; i < aLength; i++)
    {
        if (aStringValueArr[i][0] == '\0' && aLongValueArr[i] == SQL_APPEND_LONG_NULL && aDoubleValueArr[i] == SQL_APPEND_DOUBLE_NULL)
        {
            switch(aTypeArr[i])
            {
                case 11:
                    sParam[i].mUShort = SQL_APPEND_USHORT_NULL;
                    break;
                case 0:
                    sParam[i].mShort = (short)SQL_APPEND_SHORT_NULL;
                    break;
                case 12:
                    sParam[i].mUInteger = SQL_APPEND_UINTEGER_NULL;
                    break;
                case 1:
                    sParam[i].mInteger = (int)SQL_APPEND_INTEGER_NULL;
                    break;
                case 13:
                    sParam[i].mULong = SQL_APPEND_ULONG_NULL;
                    break;
                case 2:
                    sParam[i].mLong = (long long)SQL_APPEND_LONG_NULL;
                    break;
                case 3:
                    sParam[i].mFloat = SQL_APPEND_FLOAT_NULL;
                    break;
                case 4:
                    sParam[i].mDouble = SQL_APPEND_DOUBLE_NULL;
                    break;
                case 8:
                    sParam[i].mDateTime.mTime = (long long)SQL_APPEND_DATETIME_NULL;
                    sParam[i].mDateTime.mFormatStr = aDateFormat;
                    sParam[i].mDateTime.mDateStr = aStringValueArr[i];
                    break;
                case 10:
                case 9:
                case 5:
                    sParam[i].mVar.mData = aStringValueArr[i];
                    sParam[i].mVar.mLength = SQL_APPEND_TEXT_NULL;
                    break;
                case 6:
                case 7:
                    sParam[i].mIP.mLength = SQL_APPEND_IP_NULL;
                    sParam[i].mIP.mAddrString = aStringValueArr[i];
                    break;
                default:
                    break;
            }
        }
        else
        {
            switch(aTypeArr[i])
            {
                case 11:
                    sParam[i].mUShort = aLongValueArr[i];
                    break;
                case 0:
                    sParam[i].mShort = aLongValueArr[i];
                    break;
                case 12:
                    sParam[i].mUInteger = aLongValueArr[i];
                    break;
                case 1:
                    sParam[i].mInteger = aLongValueArr[i];
                    break;
                case 13:
                    sParam[i].mULong = aLongValueArr[i];
                    break;
                case 2:
                    sParam[i].mLong = aLongValueArr[i];
                    break;
                case 3:
                    sParam[i].mFloat = aDoubleValueArr[i];
                    break;
                case 4:
                    sParam[i].mDouble = aDoubleValueArr[i];
                    break;
                case 8:
                    if(aDateFormat[0] == '\0')
                    {
                        sParam[i].mDateTime.mTime = atoll(aStringValueArr[i]);
                    }
                    else
                    {
                        sParam[i].mDateTime.mTime = SQL_APPEND_DATETIME_STRING;
                        sParam[i].mDateTime.mFormatStr = aDateFormat;
                        sParam[i].mDateTime.mDateStr = aStringValueArr[i];
                    }
                    break;
                case 10:
                    sParam[i].mVar.mData = aStringValueArr[i];
                    sParam[i].mVar.mLength = aByteArrSize;
                    break;
                case 9:
                case 5:
                    sParam[i].mVar.mData = aStringValueArr[i];
                    sParam[i].mVar.mLength = strlen(aStringValueArr[i]);
                    break;
                case 6:
                case 7:
                    sParam[i].mIP.mLength = SQL_APPEND_IP_STRING;
                    sParam[i].mIP.mAddrString = aStringValueArr[i];
                    break;
                default:
                    break;
            }
        }
    }
    if( SQLAppendDataV2(aStmt, sParam) != SQL_SUCCESS )
    {
        free(sParam);
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLAppendData Error");
        return RC_FAILURE;
    }
    else
    {
        free(sParam);
        return RC_SUCCESS;
    }
}

int appendFlush(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr)
{
    if( SQLAppendFlush(aStmt) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLAppendFlush Error");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

SQLBIGINT appendClose(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr)
{
    SQLBIGINT sSuccessCount = 0;
    SQLBIGINT sFailureCount = 0;

    if( SQLAppendClose(aStmt, &sSuccessCount, &sFailureCount) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    return sSuccessCount;
}

int prepare(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr, char *aSQL)
{
    if( SQLPrepare(aStmt, (SQLCHAR *)aSQL, SQL_NTS) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLPrepare error.");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int execute(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr)
{
    if( SQLExecute(aStmt) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLExecute Error");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int colCount(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr, SQLSMALLINT *aColumnCount)
{
    if (SQLNumResultCols(aStmt, (SQLSMALLINT *)aColumnCount) != SQL_SUCCESS)
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLNumResultCols ERROR");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int describeCol(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr, char **aColumnNameArr, SQLSMALLINT *aColumnTypeArr, SQLULEN *aColumnLengArr, int aIdx)
{
    SQLSMALLINT sCol;
    SQLSMALLINT sDigit;
    SQLSMALLINT sNull;

    if(SQLDescribeCol(aStmt,
                      (SQLSMALLINT)(aIdx+1),
                      (SQLCHAR *)aColumnNameArr[aIdx], 1024,
                      (SQLSMALLINT *)&sCol,
                      (SQLSMALLINT *)&aColumnTypeArr[aIdx],
                      (SQLULEN *)&aColumnLengArr[aIdx],
                      (SQLSMALLINT *)&sDigit,
                      (SQLSMALLINT *)&sNull)
       != SQL_SUCCESS)
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLDescribeCol ERROR");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int bindCol(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr, long long *aLongDataArr, double *aDoubleDataArr, char **aStringDataArr, SQLLEN *aColumnRLengArr, int aColumnType, long aColumnLength, int aIdx)
{
    if(aColumnType == 2 ||
       aColumnType == 3 ||
       aColumnType == 4 ||
       aColumnType == 5 ||
       aColumnType == 2201 ||
       aColumnType == 2202 ||
       aColumnType == 2203 ||
       aColumnType == -5 ||
       aColumnType == -6
    )
    {
        if(SQLBindCol(aStmt,
                        aIdx + 1,
                        SQL_C_SBIGINT,
                        &aLongDataArr[aIdx],
                        0,
                        &aColumnRLengArr[aIdx]) != SQL_SUCCESS)
        {
            printError(aEnv, aCon, aStmt, aCStmtErr, "SQLBindCol ERROR");
            return RC_FAILURE;
        }
    }
    else if(aColumnType == 6 ||
            aColumnType == 7 ||
            aColumnType == 8
    )
    {
        if(SQLBindCol(aStmt,
                        aIdx + 1,
                        SQL_C_DOUBLE,
                        &aDoubleDataArr[aIdx],
                        0,
                        &aColumnRLengArr[aIdx]) != SQL_SUCCESS)
        {
            printError(aEnv, aCon, aStmt, aCStmtErr, "SQLBindCol ERROR");
            return RC_FAILURE;
        }
    }
    else
    {
        if(SQLBindCol(aStmt,
                        aIdx + 1,
                        SQL_C_CHAR,
                        aStringDataArr[aIdx],
                        aColumnLength + 1,
                        &aColumnRLengArr[aIdx]) != SQL_SUCCESS)
        {
            printError(aEnv, aCon, aStmt, aCStmtErr, "SQLBindCol ERROR");
            return RC_FAILURE;
        }
    }

    return RC_SUCCESS;
}

int fetch(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aCStmtErr)
{
    if( SQLFetch(aStmt) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aCStmtErr, "SQLFetch Error");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}
*/
import "C"

import (
    // "fmt"
    "unsafe"
    "strconv"
    "encoding/hex"
)

type MachbaseConnect struct {
    Con      C.SQLHDBC
    Env      C.SQLHENV
    CConErr  *C.char
    GConErr  string
}

type MachbaseStmt struct {
    ConPtr          *C.SQLHDBC
    EnvPtr          *C.SQLHENV
    Stmt            C.SQLHSTMT
    CStmtErr        *C.char
    GStmtErr        string
    ColumnCount     C.SQLSMALLINT
    ColumnNameArr   **C.char
    ColumnTypeArr   *C.SQLSMALLINT
    ColumnLengArr   *C.SQLULEN
    ColumnRLengArr  *C.SQLLEN
    LongDataArr     *C.longlong
    DoubleDataArr   *C.double
    StringDataArr   **C.char
}

type MachbaseSchema struct {
    Name     string
    SqlType  int
    ColType  int
    Length   int
}

const (
    RC_SUCCESS int = 0
    RC_FAILURE int = -1

    COL_SIZE      int = 1024
    MAX_COL_SIZE  int = 4096        //Byte. ... 4KB
    BLOB_COL_SIZE int = 67108864    //64MB

    MACHBASE_SHORT    int = 0
    MACHBASE_INTEGER  int = 1
    MACHBASE_LONG     int = 2
    MACHBASE_FLOAT    int = 3
    MACHBASE_DOUBLE   int = 4
    MACHBASE_VARCHAR  int = 5
    MACHBASE_IPV4     int = 6
    MACHBASE_IPV6     int = 7
    MACHBASE_DATETIME int = 8
    MACHBASE_TEXT     int = 9
    MACHBASE_BINARY   int = 10
    MACHBASE_USHORT   int = 11
    MACHBASE_UINTEGER int = 12
    MACHBASE_ULONG    int = 13

    SQL_UNKNOWN_TYPE   int = 0
    SQL_CHAR           int = 1
    SQL_NUMERIC        int = 2
    SQL_DECIMAL        int = 3
    SQL_INTEGER        int = 4
    SQL_SMALLINT       int = 5
    SQL_FLOAT          int = 6
    SQL_REAL           int = 7
    SQL_DOUBLE         int = 8
    SQL_DATETIME       int = 9
    SQL_VARCHAR        int = 12
    SQL_TYPE_DATE      int = 91
    SQL_TYPE_TIME      int = 92
    SQL_TYPE_TIMESTAMP int = 93
    SQL_CLOB           int = 2004
    SQL_BLOB           int = 2005
    SQL_TEXT           int = 2100
    SQL_IPV4           int = 2104
    SQL_IPV6           int = 2106
    SQL_USMALLINT      int = 2201
    SQL_UINTEGER       int = 2202
    SQL_UBIGINT        int = 2203
    SQL_BINARY         int = -2
    SQL_BIGINT         int = -5
    SQL_TINYINT        int = -6
)

func CreateConnect() *MachbaseConnect {
    return &MachbaseConnect{
        CConErr : nil,
        GConErr : "",
    }
}

func (sConnect *MachbaseConnect) ConnectDB(aDriver string) int {
    if sConnect == nil {
        return RC_FAILURE
    }

    sDriver := C.CString(aDriver)
    sConnect.CConErr = C.makeErrChar(C.int(MAX_COL_SIZE))
    defer C.free(unsafe.Pointer(sDriver))

    if int(C.connectDB(&sConnect.Env, &sConnect.Con, sConnect.CConErr, sDriver)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sConnect.SetConErr()
        return RC_FAILURE
    }
}

func (sConnect *MachbaseConnect) DisconnectDB() int {
    if sConnect == nil {
        return RC_FAILURE
    }

    defer func() {
        if sConnect.CConErr != nil {
            C.freeChar(sConnect.CConErr)
        }
        sConnect.CConErr = nil
    }()

    if (sConnect.Env != nil) && (sConnect.Con != nil) {
        if int(C.disconnectDB(&sConnect.Env, &sConnect.Con)) == RC_SUCCESS {
            return RC_SUCCESS
        } else {
            sConnect.GConErr = "SQLDisconnect error"
            return RC_FAILURE
        }
    } else {
        return RC_SUCCESS
    }
}

func (sConnect *MachbaseConnect) SetConErr() {
    sConnect.GConErr = C.GoString(sConnect.CConErr)
}

func (sConnect *MachbaseConnect) PrintConErr() string {
    if sConnect == nil {
        return "MachbaseConnection is nil"
    }

    return sConnect.GConErr
}

func (sConnect *MachbaseConnect) CreateStmt() *MachbaseStmt {
    if sConnect == nil {
        return nil
    }

    return &MachbaseStmt{
        ConPtr         : &sConnect.Con,
        EnvPtr         : &sConnect.Env,
        CStmtErr       : nil,
        GStmtErr       : "",
        ColumnNameArr  : nil,
        ColumnTypeArr  : nil,
        ColumnLengArr  : nil,
        ColumnRLengArr : nil,
        LongDataArr    : nil,
        DoubleDataArr  : nil,
        StringDataArr  : nil,
    }
}

func (sStmt *MachbaseStmt) AllocStmt() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    sStmt.CStmtErr = C.makeErrChar(C.int(MAX_COL_SIZE))

    if int(C.allocStmt(*sStmt.EnvPtr, *sStmt.ConPtr, &sStmt.Stmt, sStmt.CStmtErr)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) ExecDirect(aSql string) int {
    if sStmt == nil {
        return RC_FAILURE
    }

    sSql := C.CString(aSql)
    defer C.free(unsafe.Pointer(sSql))

    if int(C.execDirect(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr, sSql)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) FreeStmt() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    defer func() {
        if sStmt.CStmtErr != nil {
            C.freeChar(sStmt.CStmtErr)
        }
        sStmt.CStmtErr = nil

        if sStmt.ColumnNameArr != nil {
            C.freeCharArray(sStmt.ColumnNameArr, C.int(sStmt.ColumnCount))
        }
        sStmt.ColumnNameArr = nil

        if sStmt.ColumnTypeArr != nil {
            C.freeColTypeArray(sStmt.ColumnTypeArr)
        }
        sStmt.ColumnTypeArr = nil

        if sStmt.ColumnLengArr != nil {
            C.freeColLenArray(sStmt.ColumnLengArr)
        }
        sStmt.ColumnLengArr = nil

        if sStmt.ColumnRLengArr != nil {
            C.freeColRLenArray(sStmt.ColumnRLengArr)
        }
        sStmt.ColumnRLengArr = nil

        if sStmt.LongDataArr != nil {
            C.freeLongArray(sStmt.LongDataArr)
        }
        sStmt.LongDataArr = nil

        if sStmt.DoubleDataArr != nil {
            C.freeDoubleArray(sStmt.DoubleDataArr)
        }
        sStmt.DoubleDataArr = nil

        if sStmt.StringDataArr != nil {
            C.freeCharArray(sStmt.StringDataArr, C.int(sStmt.ColumnCount))
        }
        sStmt.StringDataArr = nil
    }()

    if sStmt.Stmt != nil {
        if int(C.freeStmt(*sStmt.EnvPtr, *sStmt.ConPtr, &sStmt.Stmt)) == RC_SUCCESS {
            return RC_SUCCESS
        } else {
            sStmt.GStmtErr = "SQLFreeStmt Error"
            return RC_FAILURE
        }
    } else {
        return RC_SUCCESS
    }
}

func (sStmt *MachbaseStmt) AppendOpen(aTableName string) int {
    if sStmt == nil {
        return RC_FAILURE
    }

    sTableName := C.CString(aTableName)
    defer C.free(unsafe.Pointer(sTableName))

    if int(C.appendOpen(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr, sTableName)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) AppendDataV2(aTypeArr []int, aValueArr []string, aDateFormat string, aLength int) int {
    if sStmt == nil {
        return RC_FAILURE
    }

    var (
        sTypeArr      *C.int   = nil
        sDateFormat   *C.char  = C.CString(aDateFormat)
        sLength       C.int    = C.int(aLength)
        sValueArr     **C.char = nil
        sConvertValue *C.char  = nil
        sByteArrSize  C.int    = C.int(0)
    )

    defer func() {
        if sValueArr != nil {
            C.freeCharArray(sValueArr, sLength)
        }
        sValueArr = nil

        if sTypeArr != nil {
            C.freeAppendTypeArray(sTypeArr)
        }
        sTypeArr = nil

        if sDateFormat != nil {
            C.free(unsafe.Pointer(sDateFormat))
        }
        sDateFormat = nil
    }()

    if (len(aValueArr) != len(aTypeArr)) || (len(aValueArr) != aLength) {
        sStmt.GStmtErr = "Array length different"
        return RC_FAILURE
    }

    sValueArr = C.makeAppendCharArray(sLength)
    sTypeArr = C.makeAppendTypeArray(sLength)

    if (sValueArr == nil) || (sTypeArr == nil) {
        sStmt.GStmtErr = "Array creation failed"
        return RC_FAILURE
    }

    for sIdx, sValue := range aValueArr {
        if sValue == "" {
            sConvertValue = C.setNilString()
        } else {
            sConvertValue = C.CString(sValue)
        }

        if aTypeArr[sIdx] == MACHBASE_BINARY {
            sByteArrSize = C.int(len([]byte(sValue)))
        }

        C.setArrayString(sValueArr, sConvertValue, C.int(sIdx))
        C.setArrayInt(sTypeArr, C.int(aTypeArr[sIdx]), C.int(sIdx))
    }

    if int(C.appendDataV2(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr, sTypeArr, sValueArr, sDateFormat, sLength, sByteArrSize)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) AppendDataV2I(aTypeArr []int, aValueArr []interface{}, aDateFormat string, aLength int) int {
    if sStmt == nil {
        return RC_FAILURE
    }

    var (
        sTypeArr            *C.int      = nil
        sDateFormat         *C.char     = C.CString(aDateFormat)
        sLength             C.int       = C.int(aLength)
        sLongValueArr       *C.longlong = nil
        sDoubleValueArr     *C.double   = nil
        sStringValueArr     **C.char    = nil
        sConvertLongValue   C.longlong  = C.SQL_APPEND_LONG_NULL
        sConvertDoubleValue C.double    = C.SQL_APPEND_DOUBLE_NULL
        sConvertStringValue *C.char     = nil
        sByteArrSize        C.int       = C.int(0)
        sResult             int         = RC_SUCCESS
    )

    defer func() {
        if sLongValueArr != nil {
            C.freeLongArray(sLongValueArr)
        }
        sLongValueArr = nil

        if sDoubleValueArr != nil {
            C.freeDoubleArray(sDoubleValueArr)
        }
        sDoubleValueArr = nil

        if sStringValueArr != nil {
            C.freeCharArray(sStringValueArr, sLength)
        }
        sStringValueArr = nil

        if sTypeArr != nil {
            C.freeAppendTypeArray(sTypeArr)
        }
        sTypeArr = nil

        if sDateFormat != nil {
            C.free(unsafe.Pointer(sDateFormat))
        }
        sDateFormat = nil
    }()

    if (len(aValueArr) != len(aTypeArr)) || (len(aValueArr) != aLength) {
        sStmt.GStmtErr = "Array length different"
        return RC_FAILURE
    }

    sLongValueArr = C.makeLongArray(sLength)
    sDoubleValueArr = C.makeDoubleArray(sLength)
    sStringValueArr = C.makeAppendCharArray(sLength)
    sTypeArr = C.makeAppendTypeArray(sLength)

    if (sLongValueArr == nil) || (sDoubleValueArr == nil) || (sStringValueArr == nil) || (sTypeArr == nil) {
        sStmt.GStmtErr = "Array creation failed"
        return RC_FAILURE
    }

    for sIdx, sValue := range aValueArr {
        sConvertLongValue = C.SQL_APPEND_LONG_NULL
        sConvertDoubleValue = C.SQL_APPEND_DOUBLE_NULL
        sConvertStringValue = C.setNilString()

        switch aTypeArr[sIdx] {
            case MACHBASE_SHORT, MACHBASE_INTEGER, MACHBASE_LONG, MACHBASE_USHORT, MACHBASE_UINTEGER, MACHBASE_ULONG:
                if sValue != nil {
                    sResult = MachTypeCheck(sValue, "INT64")
                    if sResult == RC_FAILURE {
                        sStmt.GStmtErr = "Column type and value type are different"
                        return RC_FAILURE
                    } else {
                        sConvertLongValue = C.longlong(sValue.(int64))
                    }
                }
            case MACHBASE_FLOAT, MACHBASE_DOUBLE:
                if sValue != nil {
                    sResult = MachTypeCheck(sValue, "FLOAT64")
                    if sResult == RC_FAILURE {
                        sStmt.GStmtErr = "Column type and value type are different"
                        return RC_FAILURE
                    } else {
                        sConvertDoubleValue = C.double(sValue.(float64))
                    }
                }
            case MACHBASE_VARCHAR, MACHBASE_IPV4, MACHBASE_IPV6, MACHBASE_TEXT:
                if sValue != nil {
                    sResult = MachTypeCheck(sValue, "STRING")
                    if sResult == RC_FAILURE {
                        sStmt.GStmtErr = "Column type and value type are different"
                        return RC_FAILURE
                    } else {
                        C.freeChar(sConvertStringValue)
                        sConvertStringValue = C.CString(sValue.(string))
                    }
                }
            case MACHBASE_BINARY:
                if sValue != nil {
                    sResult = MachTypeCheck(sValue, "BYTE")
                    if sResult == RC_FAILURE {
                        sStmt.GStmtErr = "Column type and value type are different"
                        return RC_FAILURE
                    } else {
                        C.freeChar(sConvertStringValue)
                        sConvertStringValue = (*C.char)(C.CBytes(sValue.([]byte)))
                        sByteArrSize = C.int(len(sValue.([]byte)))
                    }
                }
            case MACHBASE_DATETIME:
                if sValue != nil {
                    if aDateFormat == "" {
                        sResult = MachTypeCheck(sValue, "INT64")
                        if sResult == RC_FAILURE {
                            sStmt.GStmtErr = "Column type and value type are different"
                            return RC_FAILURE
                        } else {
                            C.freeChar(sConvertStringValue)
                            sConvertStringValue = C.CString(strconv.FormatInt(sValue.(int64), 10))
                        }
                    } else {
                        sResult = MachTypeCheck(sValue, "STRING")
                        if sResult == RC_FAILURE {
                            sStmt.GStmtErr = "Column type and value type are different"
                            return RC_FAILURE
                        } else {
                            C.freeChar(sConvertStringValue)
                            sConvertStringValue = C.CString(sValue.(string))
                        }
                    }
                }
        }

        C.setArrayLong(sLongValueArr, sConvertLongValue, C.int(sIdx))
        C.setArrayDouble(sDoubleValueArr, sConvertDoubleValue, C.int(sIdx))
        C.setArrayString(sStringValueArr, sConvertStringValue, C.int(sIdx))
        C.setArrayInt(sTypeArr, C.int(aTypeArr[sIdx]), C.int(sIdx))
    }

    if int(C.appendDataV2I(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr, sTypeArr, sLongValueArr, sDoubleValueArr, sStringValueArr, sDateFormat, sLength, sByteArrSize)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) AppendFlush() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    if int(C.appendFlush(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) AppendClose() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    sResult := int(C.appendClose(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr))

    if sResult == RC_FAILURE {
        return RC_FAILURE
    } else {
        sStmt.SetStmtErr()
        return sResult
    }
}

func (sStmt *MachbaseStmt) Prepare(aSql string) int {
    if sStmt == nil {
        return RC_FAILURE
    }

    sSql := C.CString(aSql)
    defer C.free(unsafe.Pointer(sSql))

    if int(C.prepare(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr, sSql)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) Execute() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    if int(C.execute(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr)) != RC_SUCCESS {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }

    if sStmt.ColCount() != RC_SUCCESS {
        return RC_FAILURE
    }

    if sStmt.DescribeCol() != RC_SUCCESS {
        return RC_FAILURE
    }

    if sStmt.BindCol() == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) ColCount() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    if int(C.colCount(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr, &sStmt.ColumnCount)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) GetColCount() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    return int(sStmt.ColumnCount)
}

func (sStmt *MachbaseStmt) DescribeCol() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    sStmt.ColumnNameArr = C.makeColNameArray(C.int(sStmt.ColumnCount), C.int(COL_SIZE))
    sStmt.ColumnTypeArr = C.makeColTypeArray(C.int(sStmt.ColumnCount))
    sStmt.ColumnLengArr = C.makeColLengthArray(C.int(sStmt.ColumnCount))
    sStmt.ColumnRLengArr = C.makeColRLengthArray(C.int(sStmt.ColumnCount))

    for i := 0; i < int(sStmt.ColumnCount); i++ {
        if int(C.describeCol(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr, sStmt.ColumnNameArr, sStmt.ColumnTypeArr, sStmt.ColumnLengArr, C.int(i))) == RC_FAILURE {
            sStmt.SetStmtErr()
            return RC_FAILURE
        }
    }

    return RC_SUCCESS
}

func (sStmt *MachbaseStmt) BindCol() int {
    if sStmt == nil {
        return RC_FAILURE
    }

    sStmt.LongDataArr = C.makeLongArray(C.int(sStmt.ColumnCount))
    sStmt.DoubleDataArr = C.makeDoubleArray(C.int(sStmt.ColumnCount))
    sStmt.StringDataArr = C.makeBindCharArray(C.int(sStmt.ColumnCount), C.int(MAX_COL_SIZE), C.int(BLOB_COL_SIZE), sStmt.ColumnTypeArr)

    for i := 0; i < int(sStmt.ColumnCount); i++ {
        if int(C.bindCol(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr, sStmt.LongDataArr, sStmt.DoubleDataArr, sStmt.StringDataArr, sStmt.ColumnRLengArr, C.getShortValue(sStmt.ColumnTypeArr, C.int(i)), C.getlongValue(sStmt.ColumnLengArr, C.int(i)), C.int(i))) == RC_FAILURE {
            sStmt.SetStmtErr()
            return RC_FAILURE
        }
    }

    return RC_SUCCESS
}

func (sStmt *MachbaseStmt) Fetch(aInterfaceArr []interface{}) int {
    if sStmt == nil {
        return RC_FAILURE
    }

    if int(C.fetch(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr)) == RC_SUCCESS {
        for i := 0; i < int(sStmt.ColumnCount); i++ {
            sColumnType := int(C.getShortValue(sStmt.ColumnTypeArr, C.int(i)))
            aInterfaceArr[i] = nil
            if sColumnType == SQL_NUMERIC ||
               sColumnType == SQL_DECIMAL ||
               sColumnType == SQL_INTEGER ||
               sColumnType == SQL_SMALLINT ||
               sColumnType == SQL_USMALLINT ||
               sColumnType == SQL_UINTEGER ||
               sColumnType == SQL_UBIGINT ||
               sColumnType == SQL_BIGINT ||
               sColumnType == SQL_TINYINT {
                if C.getColLen(sStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = int64(C.getlonglongValue(sStmt.LongDataArr, C.int(i)))
                }
            } else if sColumnType == SQL_FLOAT ||
                      sColumnType == SQL_REAL ||
                      sColumnType == SQL_DOUBLE {
                if C.getColLen(sStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = float64(C.getDoubleValue(sStmt.DoubleDataArr, C.int(i)))
                }
            } else if sColumnType == SQL_BINARY {
                if C.getColLen(sStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i], _ = hex.DecodeString(C.GoString(C.getCharValue(sStmt.StringDataArr, C.int(i))))
                }
            } else {
                if C.getColLen(sStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = C.GoString(C.getCharValue(sStmt.StringDataArr, C.int(i)))
                }
            }
        }
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) FetchMap(aInterfaceMap map[string]interface{}) int {
    if sStmt == nil {
        return RC_FAILURE
    }

    if int(C.fetch(*sStmt.EnvPtr, *sStmt.ConPtr, sStmt.Stmt, sStmt.CStmtErr)) == RC_SUCCESS {
        for i := 0; i < int(sStmt.ColumnCount); i++ {
            sColumnName := string(C.GoString(C.getCharValue(sStmt.ColumnNameArr, C.int(i))))
            sColumnType := int(C.getShortValue(sStmt.ColumnTypeArr, C.int(i)))
            aInterfaceMap[sColumnName] = nil
            if sColumnType == SQL_NUMERIC ||
               sColumnType == SQL_DECIMAL ||
               sColumnType == SQL_INTEGER ||
               sColumnType == SQL_SMALLINT ||
               sColumnType == SQL_USMALLINT ||
               sColumnType == SQL_UINTEGER ||
               sColumnType == SQL_UBIGINT ||
               sColumnType == SQL_BIGINT ||
               sColumnType == SQL_TINYINT {
                if C.getColLen(sStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceMap[sColumnName] = int64(C.getlonglongValue(sStmt.LongDataArr, C.int(i)))
                }
            } else if sColumnType == SQL_FLOAT ||
                      sColumnType == SQL_REAL ||
                      sColumnType == SQL_DOUBLE {
                if C.getColLen(sStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceMap[sColumnName] = float64(C.getDoubleValue(sStmt.DoubleDataArr, C.int(i)))
                }
            } else if sColumnType == SQL_BINARY {
                if C.getColLen(sStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceMap[sColumnName], _ = hex.DecodeString(C.GoString(C.getCharValue(sStmt.StringDataArr, C.int(i))))
                }
            } else {
                if C.getColLen(sStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceMap[sColumnName] = C.GoString(C.getCharValue(sStmt.StringDataArr, C.int(i)))
                }
            }
        }
        return RC_SUCCESS
    } else {
        sStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sStmt *MachbaseStmt) Schema() ([]MachbaseSchema, int) {
    if sStmt == nil {
        return nil, RC_FAILURE
    }

    var (
        sSchemaInfo MachbaseSchema   = MachbaseSchema{}
        sSchemaList []MachbaseSchema = nil
    )

    for i := 0; i < int(sStmt.ColumnCount); i++ {
        sSchemaInfo.Name = string(C.GoString(C.getCharValue(sStmt.ColumnNameArr, C.int(i))))
        sSchemaInfo.SqlType = int(C.getShortValue(sStmt.ColumnTypeArr, C.int(i)))
        sSchemaInfo.ColType = SchemaType(sSchemaInfo.SqlType)
        sSchemaInfo.Length = int(C.getlongValue(sStmt.ColumnLengArr, C.int(i)))
        sSchemaList = append(sSchemaList, sSchemaInfo)
    }

    return sSchemaList, RC_SUCCESS
}

func (sStmt *MachbaseStmt) SetStmtErr() {
    sStmt.GStmtErr = C.GoString(sStmt.CStmtErr)
}

func (sStmt *MachbaseStmt) PrintStmtErr() string {
    if sStmt == nil {
        return "MachbaseStmt is nil"
    }

    return sStmt.GStmtErr
}

func MachTypeCheck(aValue interface{}, aType string) int {
    var sResult int = RC_FAILURE

    if aType == "INT64" {
        switch aValue.(type) {
            case int64:
                sResult = RC_SUCCESS
            default:
                sResult = RC_FAILURE
        }
    } else if aType == "FLOAT64" {
        switch aValue.(type) {
            case float64:
                sResult = RC_SUCCESS
            default:
                sResult = RC_FAILURE
        }
    } else if aType == "STRING" {
        switch aValue.(type) {
            case string:
                sResult = RC_SUCCESS
            default:
                sResult = RC_FAILURE
        }
    } else if aType == "BYTE" {
        switch aValue.(type) {
            case []byte:
                sResult = RC_SUCCESS
            default:
                sResult = RC_FAILURE
        }
    } else {
        sResult = RC_FAILURE
    }

    return sResult
}

func SchemaType(aType int) int {
    var sResultType int = 0

    switch aType {
        case SQL_SMALLINT:    // short
            sResultType = 4
        case SQL_INTEGER:     // integer
            sResultType = 8
        case SQL_BIGINT:      // long
            sResultType = 12
        case SQL_FLOAT:       // float
            sResultType = 16
        case SQL_DOUBLE:      // double
            sResultType = 20
        case SQL_VARCHAR:     // varchar
            sResultType = 5
        case SQL_IPV4:        // ipv4
            sResultType = 32
        case SQL_IPV6:        // ipv6
            sResultType = 36
        case SQL_DATETIME:    // datetime
            sResultType = 6
        case SQL_TEXT:        // text
            sResultType = 49
        case SQL_CLOB:        // CLOB
            sResultType = 53
        case SQL_BLOB:        // BLOB
            sResultType = 57
        case SQL_BINARY:      // binary
            sResultType = 97
        case SQL_USMALLINT:   // unsigned short
            sResultType = 104
        case SQL_UINTEGER:    // unsigned integer
            sResultType = 108
        case SQL_UBIGINT:     // unsigned long
            sResultType = 112
        default:              // SQL_UNKNOWN_TYPE
            sResultType = 0
    }

    return sResultType
}