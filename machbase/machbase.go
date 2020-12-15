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

var RC_SUCCESS int = 0
var RC_FAILURE int = -1

var COL_SIZE      int = 1024
var MAX_COL_SIZE  int = 4096        //Byte. ... 4KB
var BLOB_COL_SIZE int = 67108864    //64MB

var MACHBASE_SHORT    int = 0
var MACHBASE_INTEGER  int = 1
var MACHBASE_LONG     int = 2
var MACHBASE_FLOAT    int = 3
var MACHBASE_DOUBLE   int = 4
var MACHBASE_VARCHAR  int = 5
var MACHBASE_IPV4     int = 6
var MACHBASE_IPV6     int = 7
var MACHBASE_DATETIME int = 8
var MACHBASE_TEXT     int = 9
var MACHBASE_BINARY   int = 10
var MACHBASE_USHORT   int = 11
var MACHBASE_UINTEGER int = 12
var MACHBASE_ULONG    int = 13

var SQL_UNKNOWN_TYPE   int = 0
var SQL_CHAR           int = 1
var SQL_NUMERIC        int = 2
var SQL_DECIMAL        int = 3
var SQL_INTEGER        int = 4
var SQL_SMALLINT       int = 5
var SQL_FLOAT          int = 6
var SQL_REAL           int = 7
var SQL_DOUBLE         int = 8
var SQL_DATETIME       int = 9
var SQL_VARCHAR        int = 12
var SQL_TYPE_DATE      int = 91
var SQL_TYPE_TIME      int = 92
var SQL_TYPE_TIMESTAMP int = 93
var SQL_CLOB           int = 2004
var SQL_BLOB           int = 2005
var SQL_TEXT           int = 2100
var SQL_IPV4           int = 2104
var SQL_IPV6           int = 2106
var SQL_USMALLINT      int = 2201
var SQL_UINTEGER       int = 2202
var SQL_UBIGINT        int = 2203
var SQL_BINARY         int = -2
var SQL_BIGINT         int = -5
var SQL_TINYINT        int = -6

func CreateConnect() *MachbaseConnect {
    sMachbaseConnect := &MachbaseConnect{}
    sMachbaseConnect.CConErr = nil
    sMachbaseConnect.GConErr = ""

    return sMachbaseConnect
}

func (sMachbaseConnect *MachbaseConnect) ConnectDB(aDriver string) int {
    sDriver := C.CString(aDriver)
    sMachbaseConnect.CConErr = C.makeErrChar(C.int(MAX_COL_SIZE))
    defer C.free(unsafe.Pointer(sDriver))

    if int(C.connectDB(&sMachbaseConnect.Env, &sMachbaseConnect.Con, sMachbaseConnect.CConErr, sDriver)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseConnect.SetConErr()
        return RC_FAILURE
    }
}

func (sMachbaseConnect *MachbaseConnect) DisconnectDB() int {
    defer func() {
        if sMachbaseConnect.CConErr != nil {
            C.freeChar(sMachbaseConnect.CConErr)
        }
        sMachbaseConnect.CConErr = nil
    }()

    if int(C.disconnectDB(&sMachbaseConnect.Env, &sMachbaseConnect.Con)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseConnect.GConErr = "SQLDisconnect error"
        return RC_FAILURE
    }
}

func (sMachbaseConnect *MachbaseConnect) SetConErr() {
    sMachbaseConnect.GConErr = C.GoString(sMachbaseConnect.CConErr)
}

func (sMachbaseConnect *MachbaseConnect) PrintConErr() string {
    return sMachbaseConnect.GConErr
}

func (sMachbaseConnect *MachbaseConnect) CreateStmt() *MachbaseStmt {
    sMachbaseStmt := &MachbaseStmt{}
    sMachbaseStmt.ConPtr = &sMachbaseConnect.Con
    sMachbaseStmt.EnvPtr = &sMachbaseConnect.Env
    sMachbaseStmt.CStmtErr = nil
    sMachbaseStmt.GStmtErr = ""
    sMachbaseStmt.ColumnNameArr = nil
    sMachbaseStmt.ColumnTypeArr = nil
    sMachbaseStmt.ColumnLengArr = nil
    sMachbaseStmt.ColumnRLengArr = nil
    sMachbaseStmt.LongDataArr = nil
    sMachbaseStmt.DoubleDataArr = nil
    sMachbaseStmt.StringDataArr = nil

    return sMachbaseStmt
}

func (sMachbaseStmt *MachbaseStmt) AllocStmt() int {
    sMachbaseStmt.CStmtErr = C.makeErrChar(C.int(MAX_COL_SIZE))

    if int(C.allocStmt(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, &sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) ExecDirect(aSql string) int {
    sSql := C.CString(aSql)
    defer C.free(unsafe.Pointer(sSql))

    if int(C.execDirect(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr, sSql)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) FreeStmt() int {
    defer func() {
        if sMachbaseStmt.CStmtErr != nil {
            C.freeChar(sMachbaseStmt.CStmtErr)
        }
        sMachbaseStmt.CStmtErr = nil

        if sMachbaseStmt.ColumnNameArr != nil {
            C.freeCharArray(sMachbaseStmt.ColumnNameArr, C.int(sMachbaseStmt.ColumnCount))
        }
        sMachbaseStmt.ColumnNameArr = nil

        if sMachbaseStmt.ColumnTypeArr != nil {
            C.freeColTypeArray(sMachbaseStmt.ColumnTypeArr)
        }
        sMachbaseStmt.ColumnTypeArr = nil

        if sMachbaseStmt.ColumnLengArr != nil {
            C.freeColLenArray(sMachbaseStmt.ColumnLengArr)
        }
        sMachbaseStmt.ColumnLengArr = nil

        if sMachbaseStmt.ColumnRLengArr != nil {
            C.freeColRLenArray(sMachbaseStmt.ColumnRLengArr)
        }
        sMachbaseStmt.ColumnRLengArr = nil

        if sMachbaseStmt.LongDataArr != nil {
            C.freeLongArray(sMachbaseStmt.LongDataArr)
        }
        sMachbaseStmt.LongDataArr = nil

        if sMachbaseStmt.DoubleDataArr != nil {
            C.freeDoubleArray(sMachbaseStmt.DoubleDataArr)
        }
        sMachbaseStmt.DoubleDataArr = nil

        if sMachbaseStmt.StringDataArr != nil {
            C.freeCharArray(sMachbaseStmt.StringDataArr, C.int(sMachbaseStmt.ColumnCount))
        }
        sMachbaseStmt.StringDataArr = nil
    }()

    if int(C.freeStmt(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, &sMachbaseStmt.Stmt)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.GStmtErr = "SQLFreeStmt Error"
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendOpen(aTableName string) int {
    sTableName := C.CString(aTableName)
    defer C.free(unsafe.Pointer(sTableName))

    if int(C.appendOpen(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr, sTableName)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendDataV2(aTypeArr []int, aValueArr []string, aDateFormat string, aLength int) int {
    var sTypeArr *C.int = nil
    var sDateFormat *C.char = C.CString(aDateFormat)
    var sLength C.int = C.int(aLength)
    var sValueArr **C.char = nil
    var sConvertValue *C.char = nil
    var sByteArrSize C.int = C.int(0)

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
        sMachbaseStmt.GStmtErr = "Array length different"
        return RC_FAILURE
    }

    sValueArr = C.makeAppendCharArray(sLength)
    sTypeArr = C.makeAppendTypeArray(sLength)

    if (sValueArr == nil) || (sTypeArr == nil) {
        sMachbaseStmt.GStmtErr = "Array creation failed"
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

    if int(C.appendDataV2(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr, sTypeArr, sValueArr, sDateFormat, sLength, sByteArrSize)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendDataV2I(aTypeArr []int, aValueArr []interface{}, aDateFormat string, aLength int) int {
    var sTypeArr *C.int = nil
    var sDateFormat *C.char = C.CString(aDateFormat)
    var sLength C.int = C.int(aLength)
    var sLongValueArr *C.longlong = nil
    var sDoubleValueArr *C.double = nil
    var sStringValueArr **C.char = nil
    var sConvertLongValue C.longlong = C.SQL_APPEND_LONG_NULL
    var sConvertDoubleValue C.double = C.SQL_APPEND_DOUBLE_NULL
    var sConvertStringValue *C.char = nil
    var sByteArrSize C.int = C.int(0)
    var sResult int = RC_SUCCESS

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
        sMachbaseStmt.GStmtErr = "Array length different"
        return RC_FAILURE
    }

    sLongValueArr = C.makeLongArray(sLength)
    sDoubleValueArr = C.makeDoubleArray(sLength)
    sStringValueArr = C.makeAppendCharArray(sLength)
    sTypeArr = C.makeAppendTypeArray(sLength)

    if (sLongValueArr == nil) || (sDoubleValueArr == nil) || (sStringValueArr == nil) || (sTypeArr == nil) {
        sMachbaseStmt.GStmtErr = "Array creation failed"
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
                        sMachbaseStmt.GStmtErr = "Column type and value type are different"
                        return RC_FAILURE
                    } else {
                        sConvertLongValue = C.longlong(sValue.(int64))
                    }
                }
            case MACHBASE_FLOAT, MACHBASE_DOUBLE:
                if sValue != nil {
                    sResult = MachTypeCheck(sValue, "FLOAT64")
                    if sResult == RC_FAILURE {
                        sMachbaseStmt.GStmtErr = "Column type and value type are different"
                        return RC_FAILURE
                    } else {
                        sConvertDoubleValue = C.double(sValue.(float64))
                    }
                }
            case MACHBASE_VARCHAR, MACHBASE_IPV4, MACHBASE_IPV6, MACHBASE_TEXT:
                if sValue != nil {
                    sResult = MachTypeCheck(sValue, "STRING")
                    if sResult == RC_FAILURE {
                        sMachbaseStmt.GStmtErr = "Column type and value type are different"
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
                        sMachbaseStmt.GStmtErr = "Column type and value type are different"
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
                            sMachbaseStmt.GStmtErr = "Column type and value type are different"
                            return RC_FAILURE
                        } else {
                            C.freeChar(sConvertStringValue)
                            sConvertStringValue = C.CString(strconv.FormatInt(sValue.(int64), 10))
                        }
                    } else {
                        sResult = MachTypeCheck(sValue, "STRING")
                        if sResult == RC_FAILURE {
                            sMachbaseStmt.GStmtErr = "Column type and value type are different"
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

    if int(C.appendDataV2I(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr, sTypeArr, sLongValueArr, sDoubleValueArr, sStringValueArr, sDateFormat, sLength, sByteArrSize)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendFlush() int {
    if int(C.appendFlush(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendClose() int {
    sResult := int(C.appendClose(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr))

    if sResult == RC_FAILURE {
        return RC_FAILURE
    } else {
        sMachbaseStmt.SetStmtErr()
        return sResult
    }
}

func (sMachbaseStmt *MachbaseStmt) Prepare(aSql string) int {
    sSql := C.CString(aSql)
    defer C.free(unsafe.Pointer(sSql))

    if int(C.prepare(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr, sSql)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) Execute() int {
    if int(C.execute(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr)) != RC_SUCCESS {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }

    if sMachbaseStmt.ColCount() != RC_SUCCESS {
        return RC_FAILURE
    }

    if sMachbaseStmt.DescribeCol() != RC_SUCCESS {
        return RC_FAILURE
    }

    if sMachbaseStmt.BindCol() == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) ColCount() int {
    if int(C.colCount(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr, &sMachbaseStmt.ColumnCount)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) GetColCount() int {
    return int(sMachbaseStmt.ColumnCount)
}

func (sMachbaseStmt *MachbaseStmt) DescribeCol() int {
    sMachbaseStmt.ColumnNameArr = C.makeColNameArray(C.int(sMachbaseStmt.ColumnCount), C.int(COL_SIZE))
    sMachbaseStmt.ColumnTypeArr = C.makeColTypeArray(C.int(sMachbaseStmt.ColumnCount))
    sMachbaseStmt.ColumnLengArr = C.makeColLengthArray(C.int(sMachbaseStmt.ColumnCount))
    sMachbaseStmt.ColumnRLengArr = C.makeColRLengthArray(C.int(sMachbaseStmt.ColumnCount))

    for i := 0; i < int(sMachbaseStmt.ColumnCount); i++ {
        if int(C.describeCol(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr, sMachbaseStmt.ColumnNameArr, sMachbaseStmt.ColumnTypeArr, sMachbaseStmt.ColumnLengArr, C.int(i))) == RC_FAILURE {
            sMachbaseStmt.SetStmtErr()
            return RC_FAILURE
        }
    }

    return RC_SUCCESS
}

func (sMachbaseStmt *MachbaseStmt) BindCol() int {
    sMachbaseStmt.LongDataArr = C.makeLongArray(C.int(sMachbaseStmt.ColumnCount))
    sMachbaseStmt.DoubleDataArr = C.makeDoubleArray(C.int(sMachbaseStmt.ColumnCount))
    sMachbaseStmt.StringDataArr = C.makeBindCharArray(C.int(sMachbaseStmt.ColumnCount), C.int(MAX_COL_SIZE), C.int(BLOB_COL_SIZE), sMachbaseStmt.ColumnTypeArr)

    for i := 0; i < int(sMachbaseStmt.ColumnCount); i++ {
        if int(C.bindCol(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr, sMachbaseStmt.LongDataArr, sMachbaseStmt.DoubleDataArr, sMachbaseStmt.StringDataArr, sMachbaseStmt.ColumnRLengArr, C.getShortValue(sMachbaseStmt.ColumnTypeArr, C.int(i)), C.getlongValue(sMachbaseStmt.ColumnLengArr, C.int(i)), C.int(i))) == RC_FAILURE {
            sMachbaseStmt.SetStmtErr()
            return RC_FAILURE
        }
    }

    return RC_SUCCESS
}

func (sMachbaseStmt *MachbaseStmt) Fetch(aInterfaceArr []interface{}) int {
    if int(C.fetch(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr)) == RC_SUCCESS {
        for i := 0; i < int(sMachbaseStmt.ColumnCount); i++ {
            sColumnType := int(C.getShortValue(sMachbaseStmt.ColumnTypeArr, C.int(i)))
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
                if C.getColLen(sMachbaseStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = int64(C.getlonglongValue(sMachbaseStmt.LongDataArr, C.int(i)))
                }
            } else if sColumnType == SQL_FLOAT ||
                      sColumnType == SQL_REAL ||
                      sColumnType == SQL_DOUBLE {
                if C.getColLen(sMachbaseStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = float64(C.getDoubleValue(sMachbaseStmt.DoubleDataArr, C.int(i)))
                }
            } else if sColumnType == SQL_BINARY {
                if C.getColLen(sMachbaseStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i], _ = hex.DecodeString(C.GoString(C.getCharValue(sMachbaseStmt.StringDataArr, C.int(i))))
                }
            } else {
                if C.getColLen(sMachbaseStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = C.GoString(C.getCharValue(sMachbaseStmt.StringDataArr, C.int(i)))
                }
            }
        }
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) FetchMap(aInterfaceMap map[string]interface{}) int {
    if int(C.fetch(*sMachbaseStmt.EnvPtr, *sMachbaseStmt.ConPtr, sMachbaseStmt.Stmt, sMachbaseStmt.CStmtErr)) == RC_SUCCESS {
        for i := 0; i < int(sMachbaseStmt.ColumnCount); i++ {
            sColumnName := string(C.GoString(C.getCharValue(sMachbaseStmt.ColumnNameArr, C.int(i))))
            sColumnType := int(C.getShortValue(sMachbaseStmt.ColumnTypeArr, C.int(i)))
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
                if C.getColLen(sMachbaseStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceMap[sColumnName] = int64(C.getlonglongValue(sMachbaseStmt.LongDataArr, C.int(i)))
                }
            } else if sColumnType == SQL_FLOAT ||
                      sColumnType == SQL_REAL ||
                      sColumnType == SQL_DOUBLE {
                if C.getColLen(sMachbaseStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceMap[sColumnName] = float64(C.getDoubleValue(sMachbaseStmt.DoubleDataArr, C.int(i)))
                }
            } else if sColumnType == SQL_BINARY {
                if C.getColLen(sMachbaseStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceMap[sColumnName], _ = hex.DecodeString(C.GoString(C.getCharValue(sMachbaseStmt.StringDataArr, C.int(i))))
                }
            } else {
                if C.getColLen(sMachbaseStmt.ColumnRLengArr, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceMap[sColumnName] = C.GoString(C.getCharValue(sMachbaseStmt.StringDataArr, C.int(i)))
                }
            }
        }
        return RC_SUCCESS
    } else {
        sMachbaseStmt.SetStmtErr()
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) Schema() ([]MachbaseSchema, int) {
    var sSchemaInfo MachbaseSchema = MachbaseSchema{}
    var sSchemaList []MachbaseSchema = nil

    for i := 0; i < int(sMachbaseStmt.ColumnCount); i++ {
        sSchemaInfo.Name = string(C.GoString(C.getCharValue(sMachbaseStmt.ColumnNameArr, C.int(i))))
        sSchemaInfo.SqlType = int(C.getShortValue(sMachbaseStmt.ColumnTypeArr, C.int(i)))
        sSchemaInfo.ColType = SchemaType(sSchemaInfo.SqlType)
        sSchemaInfo.Length = int(C.getlongValue(sMachbaseStmt.ColumnLengArr, C.int(i)))
        sSchemaList = append(sSchemaList, sSchemaInfo)
    }

    return sSchemaList, RC_SUCCESS
}

func (sMachbaseStmt *MachbaseStmt) SetStmtErr() {
    sMachbaseStmt.GStmtErr = C.GoString(sMachbaseStmt.CStmtErr)
}

func (sMachbaseStmt *MachbaseStmt) PrintStmtErr() string {
    return sMachbaseStmt.GStmtErr
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