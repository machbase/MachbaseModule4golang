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
static char** makeCharArray(int size)
{
    return calloc(size, sizeof(char*));
}

static char** makeCharArray2(int size, int size2)
{
    char** sRet = calloc(size, sizeof(char*));
    int i = 0;

    for(i = 0; i < size; i++)
    {
        sRet[i] = (char *)malloc(sizeof(char) * size2);
    }

    return sRet;
}

static char* makeErrChar(int size)
{
    return malloc(sizeof(char) * size);
}

static int* makeIntArray(int size)
{
    return calloc(size, sizeof(int));
}

static SQLSMALLINT* makeIntArray2(int size)
{
    return calloc(size, sizeof(SQLSMALLINT));
}

static SQLULEN* makeIntArray3(int size)
{
    return calloc(size, sizeof(SQLULEN));
}

static SQLLEN* makeIntArray4(int size)
{
    return calloc(size, sizeof(SQLLEN));
}

static long long* makeLongArray(int size)
{
    return calloc(size, sizeof(long long));
}

static double* makeDoubleArray(int size)
{
    return calloc(size, sizeof(double));
}

//******************************************************* pointer array data setting
static void setArrayString(char **a, char *s, int n)
{
    a[n] = s;
}

static void setArrayInt(int *a, int s, int n)
{
    a[n] = s;
}

static char* setNilString()
{
    char* sResult = malloc(sizeof(char) * 2);
    sResult[0] = 0;

    return sResult;
}

//******************************************************* pointer array free
static void freeCharArray(char **a, int size)
{
    int i;
    for (i = 0; i < size; i++)
        free(a[i]);
    free(a);
}

static void freeErrChar(char *a)
{
    free(a);
}

static void freeIntArray(int *a)
{
    free(a);
}

static void freeIntArray2(SQLSMALLINT *a)
{
    free(a);
}

static void freeIntArray3(SQLULEN *a)
{
    free(a);
}

static void freeIntArray4(SQLLEN *a)
{
    free(a);
}

static void freeLongArray(long long *a)
{
    free(a);
}

static void freeDoubleArray(double *a)
{
    free(a);
}

//******************************************************* Get data from pointer array start
static char* getCharValue(char** aStr, int aIndex)
{
    return aStr[aIndex];
}

static SQLLEN getColLen(SQLLEN* aLen, int aIndex)
{
    return aLen[aIndex];
}

static int getShortValue(SQLSMALLINT* aInt, int aIndex)
{
    return (int)aInt[aIndex];
}

static long getlongValue(SQLULEN* aLong, int aIndex)
{
    return (long)aLong[aIndex];
}

static long long getlonglongValue(long long* aLong, int aIndex)
{
    return (long long)aLong[aIndex];
}

static double getDoubleValue(double* adouble, int aIndex)
{
    return (double)adouble[aIndex];
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

int connectDB(SQLHENV *aEnv, SQLHDBC *aCon, char *aErrCon, char *aConnStr)
{
    if( SQLAllocEnv(aEnv) != SQL_SUCCESS )
    {
        strcpy(aErrCon, "SQLAllocEnv error");

        return RC_FAILURE;
    }

    if( SQLAllocConnect(*aEnv, aCon) != SQL_SUCCESS )
    {
        strcpy(aErrCon, "SQLAllocConnect error");

        SQLFreeEnv(*aEnv);
        *aEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    if( SQLDriverConnect( *aCon, NULL,
                          (SQLCHAR *)aConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {
        printError(*aEnv, *aCon, NULL, aErrCon, "SQLDriverConnect error");

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
        printf("%s\n", "SQLDisconnect error");
        return RC_FAILURE;
    }

    SQLFreeConnect(*aCon);
    *aCon = SQL_NULL_HDBC;

    SQLFreeEnv(*aEnv);
    *aEnv = SQL_NULL_HENV;

    return RC_SUCCESS;
}

int allocStmt(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT *aStmt, char *aErrStmt, int aErrIgnore)
{
    *aStmt = SQL_NULL_HSTMT;

    if( SQLAllocStmt(aCon, aStmt) != SQL_SUCCESS )
    {
        if( aErrIgnore == 0 )
        {
            printError(aEnv, aCon, *aStmt, aErrStmt, "SQLAllocStmt Error");
        }

        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int execDirect(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt, char *aSQL, int aErrIgnore)
{
    if( SQLExecDirect(aStmt, (SQLCHAR *)aSQL, SQL_NTS) != SQL_SUCCESS )
    {
        if( aErrIgnore == 0 )
        {
            printError(aEnv, aCon, aStmt, aErrStmt, "SQLExecDirect Error");
        }

        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int freeStmt(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT *aStmt, int aErrIgnore)
{
    if( SQLFreeStmt(*aStmt, SQL_DROP) != SQL_SUCCESS )
    {
        if (aErrIgnore == 0)
        {
            printf("%s\n", "SQLFreeStmt Error");
        }

        return RC_FAILURE;
    }

    *aStmt = SQL_NULL_HSTMT;
    return RC_SUCCESS;
}

int appendOpen(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt, char *aTableName)
{
    if( SQLAppendOpen(aStmt, (SQLCHAR *)aTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLAppendOpen Error");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int appendDataV2(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt, int *aType, char **aValue, char *aDateFormat, int aLen)
{
    int i;
    SQL_APPEND_PARAM *sParam = malloc(sizeof(SQL_APPEND_PARAM) * aLen);
    memset(sParam,0,sizeof(SQL_APPEND_PARAM) * aLen);
    for(i=0; i < aLen; i++)
    {
        if (aValue[i][0] == '\0')
        {
            switch(aType[i])
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
                    sParam[i].mDateTime.mDateStr = aValue[i];
                    break;
                case 9:
                case 10:
                case 5:
                    sParam[i].mVar.mData = aValue[i];
                    sParam[i].mVar.mLength = SQL_APPEND_TEXT_NULL;
                    break;
                case 6:
                case 7:
                    sParam[i].mIP.mLength = SQL_APPEND_IP_NULL;
                    sParam[i].mIP.mAddrString = aValue[i];
                    break;
                default:
                    break;
            }
        }
        else
        {
            switch(aType[i])
            {
                case 11:
                    sParam[i].mUShort = atoi(aValue[i]);
                    break;
                case 0:
                    sParam[i].mShort = atoi(aValue[i]);
                    break;
                case 12:
                    sParam[i].mUInteger = atoi(aValue[i]);
                    break;
                case 1:
                    sParam[i].mInteger = atoi(aValue[i]);
                    break;
                case 13:
                    sParam[i].mULong = atoll(aValue[i]);
                    break;
                case 2:
                    sParam[i].mLong = atoll(aValue[i]);
                    break;
                case 3:
                    sParam[i].mFloat = atof(aValue[i]);
                    break;
                case 4:
                    sParam[i].mDouble = atof(aValue[i]);
                    break;
                case 8:
                    if(aDateFormat[0] == '\0')
                    {
                        sParam[i].mDateTime.mTime = atoll(aValue[i]);
                    }
                    else
                    {
                        sParam[i].mDateTime.mTime = SQL_APPEND_DATETIME_STRING;
                        sParam[i].mDateTime.mFormatStr = aDateFormat;
                        sParam[i].mDateTime.mDateStr = aValue[i];
                    }
                    break;
                case 9:
                case 10:
                case 5:
                    sParam[i].mVar.mData = aValue[i];
                    sParam[i].mVar.mLength = strlen(aValue[i]);
                    break;
                case 6:
                case 7:
                    sParam[i].mIP.mLength = SQL_APPEND_IP_STRING;
                    sParam[i].mIP.mAddrString = aValue[i];
                    break;
                default:
                    break;
            }
        }
    }

    if( SQLAppendDataV2(aStmt, sParam) != SQL_SUCCESS )
    {
        free(sParam);
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLAppendData Error");
        return RC_FAILURE;
    }
    else
    {
        free(sParam);
        return RC_SUCCESS;
    }
}

int appendFlush(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt)
{
    if( SQLAppendFlush(aStmt) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLAppendFlush Error");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

SQLBIGINT appendClose(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt)
{
    SQLBIGINT sSuccessCount = 0;
    SQLBIGINT sFailureCount = 0;

    if( SQLAppendClose(aStmt, &sSuccessCount, &sFailureCount) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    return sSuccessCount;
}

int prepare(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt, char *aSQL)
{
    if( SQLPrepare(aStmt, (SQLCHAR *)aSQL, SQL_NTS) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLPrepare Error");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int execute(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt)
{
    if( SQLExecute(aStmt) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLExecute Error");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int colCount(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt, SQLSMALLINT *aCol)
{
    if (SQLNumResultCols(aStmt, (SQLSMALLINT *)aCol) != SQL_SUCCESS)
    {
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLNumResultCols ERROR");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int describeCol(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt, char **aColName, SQLSMALLINT *aColType, SQLULEN *aColLen, int aNum)
{
    SQLSMALLINT sCol;
    SQLSMALLINT sDigit;
    SQLSMALLINT sNull;

    if(SQLDescribeCol(aStmt,
                      (SQLSMALLINT)(aNum+1),
                      (SQLCHAR *)aColName[aNum], 1024,
                      (SQLSMALLINT *)&sCol,
                      (SQLSMALLINT *)&aColType[aNum],
                      (SQLULEN *)&aColLen[aNum],
                      (SQLSMALLINT *)&sDigit,
                      (SQLSMALLINT *)&sNull)
       != SQL_SUCCESS)
    {
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLDescribeCol ERROR");
        return RC_FAILURE;
    }
    else
    {
        return RC_SUCCESS;
    }
}

int bindCol(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt, long long *aLongData, double *aDoubleData, char **aStrData, SQLLEN *aColRLen, int aColType, long aColLen, int aNum)
{
    if(aColType == 2 ||
       aColType == 3 ||
       aColType == 4 ||
       aColType == 5 ||
       aColType == 2201 ||
       aColType == 2202 ||
       aColType == 2203 ||
       aColType == -5 ||
       aColType == -6
    )
    {
        if(SQLBindCol(aStmt,
                        aNum + 1,
                        SQL_C_SBIGINT,
                        &aLongData[aNum],
                        0,
                        &aColRLen[aNum]) != SQL_SUCCESS)
        {
            printError(aEnv, aCon, aStmt, aErrStmt, "SQLBindCol ERROR");
            return RC_FAILURE;
        }
    }
    else if(aColType == 6 ||
            aColType == 7 ||
            aColType == 8
    )
    {
        if(SQLBindCol(aStmt,
                        aNum + 1,
                        SQL_C_DOUBLE,
                        &aDoubleData[aNum],
                        0,
                        &aColRLen[aNum]) != SQL_SUCCESS)
        {
            printError(aEnv, aCon, aStmt, aErrStmt, "SQLBindCol ERROR");
            return RC_FAILURE;
        }
    }
    else
    {
        if(SQLBindCol(aStmt,
                        aNum + 1,
                        SQL_C_CHAR,
                        aStrData[aNum],
                        aColLen + 1,
                        &aColRLen[aNum]) != SQL_SUCCESS)
        {
            printError(aEnv, aCon, aStmt, aErrStmt, "SQLBindCol ERROR");
            return RC_FAILURE;
        }
    }

    return RC_SUCCESS;
}

int fetch(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aErrStmt)
{
    if( SQLFetch(aStmt) != SQL_SUCCESS )
    {
        printError(aEnv, aCon, aStmt, aErrStmt, "SQLFetch Error");
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
    "unsafe"
    "encoding/hex"
    // "fmt"
)

type MachbaseConnect struct {
    sCon     C.SQLHDBC
    sEnv     C.SQLHENV
    sErrCon  *C.char
}

type MachbaseStmt struct {
    sConP        *C.SQLHDBC
    sEnvP        *C.SQLHENV
    sStmt        C.SQLHSTMT
    sErrStmt     *C.char
    sColCount    C.SQLSMALLINT
    sColName     **C.char
    sColType     *C.SQLSMALLINT
    sColLen      *C.SQLULEN
    sLongData    *C.longlong
    sDoubleData  *C.double
    sStrData     **C.char
    sColRLen     *C.SQLLEN
}

var RC_SUCCESS int = 0
var RC_FAILURE int = -1

var COL_SIZE int = 1024
var MAX_COL_SIZE int = 4096

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
    sMachbaseConnect.sErrCon = nil

    return sMachbaseConnect
}

func (sMachbaseConnect *MachbaseConnect) ConnectDB(aDriver string) int {
    sDriver := C.CString(aDriver)
    sMachbaseConnect.sErrCon = C.makeErrChar(C.int(MAX_COL_SIZE))
    defer C.free(unsafe.Pointer(sDriver))

    if int(C.connectDB(&sMachbaseConnect.sEnv, &sMachbaseConnect.sCon, sMachbaseConnect.sErrCon, sDriver)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseConnect *MachbaseConnect) DisconnectDB() int {
    defer func() {
        if sMachbaseConnect.sErrCon != nil {
            C.freeErrChar(sMachbaseConnect.sErrCon)
        }
        sMachbaseConnect.sErrCon = nil
    }()

    if int(C.disconnectDB(&sMachbaseConnect.sEnv, &sMachbaseConnect.sCon)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseConnect *MachbaseConnect) PrintConErr() string {
    return C.GoString(sMachbaseConnect.sErrCon)
}

func (sMachbaseConnect *MachbaseConnect) CreateStmt() *MachbaseStmt {
    sMachbaseStmt := &MachbaseStmt{}
    sMachbaseStmt.sConP = &sMachbaseConnect.sCon
    sMachbaseStmt.sEnvP = &sMachbaseConnect.sEnv
    sMachbaseStmt.sErrStmt = nil
    sMachbaseStmt.sColName = nil
    sMachbaseStmt.sColType = nil
    sMachbaseStmt.sColLen = nil
    sMachbaseStmt.sLongData = nil
    sMachbaseStmt.sDoubleData = nil
    sMachbaseStmt.sStrData = nil
    sMachbaseStmt.sColRLen = nil

    return sMachbaseStmt
}

func (sMachbaseStmt *MachbaseStmt) AllocStmt() int {
    sMachbaseStmt.sErrStmt = C.makeErrChar(C.int(MAX_COL_SIZE))

    if int(C.allocStmt(*sMachbaseStmt.sEnvP ,*sMachbaseStmt.sConP, &sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt, 0)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) ExecDirect(aSql string) int {
    sSql := C.CString(aSql)
    defer C.free(unsafe.Pointer(sSql))

    if int(C.execDirect(*sMachbaseStmt.sEnvP ,*sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt, sSql, 0)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) FreeStmt() int {
    defer func() {
        if sMachbaseStmt.sErrStmt != nil {
            C.freeErrChar(sMachbaseStmt.sErrStmt)
        }
        sMachbaseStmt.sErrStmt = nil

        if sMachbaseStmt.sColName != nil {
            C.freeCharArray(sMachbaseStmt.sColName, C.int(sMachbaseStmt.sColCount))
        }
        sMachbaseStmt.sColName = nil

        if sMachbaseStmt.sColType != nil {
            C.freeIntArray2(sMachbaseStmt.sColType)
        }
        sMachbaseStmt.sColType = nil

        if sMachbaseStmt.sColLen != nil {
            C.freeIntArray3(sMachbaseStmt.sColLen)
        }
        sMachbaseStmt.sColLen = nil

        if sMachbaseStmt.sLongData != nil {
            C.freeLongArray(sMachbaseStmt.sLongData)
        }
        sMachbaseStmt.sLongData = nil

        if sMachbaseStmt.sDoubleData != nil {
            C.freeDoubleArray(sMachbaseStmt.sDoubleData)
        }
        sMachbaseStmt.sDoubleData = nil

        if sMachbaseStmt.sStrData != nil {
            C.freeCharArray(sMachbaseStmt.sStrData, C.int(sMachbaseStmt.sColCount))
        }
        sMachbaseStmt.sStrData = nil

        if sMachbaseStmt.sColRLen != nil {
            C.freeIntArray4(sMachbaseStmt.sColRLen)
        }
        sMachbaseStmt.sColRLen = nil
    }()

    if int(C.freeStmt(*sMachbaseStmt.sEnvP ,*sMachbaseStmt.sConP, &sMachbaseStmt.sStmt, 0)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendOpen(aTableName string) int {
    sTableName := C.CString(aTableName)
    defer C.free(unsafe.Pointer(sTableName))

    if int(C.appendOpen(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt, sTableName)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendDataV2(aType []int, aValue []string, aDateFormat string, aLen int) int {
    if len(aValue) != len(aType) {
        return RC_FAILURE
    }

    var sValue **C.char = nil
    var sType *C.int = nil
    var sDateFormat *C.char = nil
    var sConvertValue *C.char = nil

    sValue = C.makeCharArray(C.int(len(aValue)))
    if sValue == nil {
        return RC_FAILURE
    }

    sType = C.makeIntArray(C.int(len(aType)))
    if sType == nil {
        return RC_FAILURE
    }

    sDateFormat = C.CString(aDateFormat)
    sLen := C.int(aLen)

    for i, s := range aValue {
        if s == "" {
            sConvertValue = C.setNilString()
        } else {
            sConvertValue = C.CString(s)
        }

        C.setArrayString(sValue, sConvertValue, C.int(i))
    }

    for i, s := range aType {
        C.setArrayInt(sType, C.int(s), C.int(i))
    }

    defer func() {
        if sValue != nil {
            C.freeCharArray(sValue, C.int(len(aValue)))
        }

        if sType != nil {
            C.freeIntArray(sType)
        }

        if sDateFormat != nil {
            C.free(unsafe.Pointer(sDateFormat))
        }
    }()

    if int(C.appendDataV2(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt, sType, sValue, sDateFormat, sLen)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendFlush() int {
    if int(C.appendFlush(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) AppendClose() int {
    sResult := int(C.appendClose(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt))

    if sResult == RC_FAILURE {
        return RC_FAILURE
    } else {
        return sResult
    }
}

func (sMachbaseStmt *MachbaseStmt) Prepare(aSql string) int {
    sSql := C.CString(aSql)
    defer C.free(unsafe.Pointer(sSql))

    if int(C.prepare(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt, sSql)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) Execute() int {
    if int(C.execute(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt)) != RC_SUCCESS {
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
    if int(C.colCount(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt, &sMachbaseStmt.sColCount)) == RC_SUCCESS {
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) GetColCount() int {
    return int(sMachbaseStmt.sColCount)
}

func (sMachbaseStmt *MachbaseStmt) DescribeCol() int {
    sMachbaseStmt.sColName = C.makeCharArray2(C.int(sMachbaseStmt.sColCount), C.int(COL_SIZE))
    sMachbaseStmt.sColType = C.makeIntArray2(C.int(sMachbaseStmt.sColCount))
    sMachbaseStmt.sColLen = C.makeIntArray3(C.int(sMachbaseStmt.sColCount))
    sMachbaseStmt.sColRLen = C.makeIntArray4(C.int(sMachbaseStmt.sColCount))

    for i := 0; i < int(sMachbaseStmt.sColCount); i++ {
        if int(C.describeCol(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt, sMachbaseStmt.sColName, sMachbaseStmt.sColType, sMachbaseStmt.sColLen, C.int(i))) == RC_FAILURE {
            return RC_FAILURE
        }
    }

    return RC_SUCCESS
}

func (sMachbaseStmt *MachbaseStmt) BindCol() int {
    sMachbaseStmt.sLongData = C.makeLongArray(C.int(sMachbaseStmt.sColCount))
    sMachbaseStmt.sDoubleData = C.makeDoubleArray(C.int(sMachbaseStmt.sColCount))
    sMachbaseStmt.sStrData = C.makeCharArray2(C.int(sMachbaseStmt.sColCount), C.int(MAX_COL_SIZE))

    for i := 0; i < int(sMachbaseStmt.sColCount); i++ {
        if int(C.bindCol(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt, sMachbaseStmt.sLongData, sMachbaseStmt.sDoubleData, sMachbaseStmt.sStrData, sMachbaseStmt.sColRLen, C.getShortValue(sMachbaseStmt.sColType, C.int(i)), C.getlongValue(sMachbaseStmt.sColLen, C.int(i)), C.int(i))) == RC_FAILURE {
            return RC_FAILURE
        }
    }

    return RC_SUCCESS
}

func (sMachbaseStmt *MachbaseStmt) Fetch(aInterfaceArr []interface{}) int {
    if int(C.fetch(*sMachbaseStmt.sEnvP, *sMachbaseStmt.sConP, sMachbaseStmt.sStmt, sMachbaseStmt.sErrStmt)) == RC_SUCCESS {
        for i := 0; i < int(sMachbaseStmt.sColCount); i++ {
            sCType := int(C.getShortValue(sMachbaseStmt.sColType, C.int(i)))
            aInterfaceArr[i] = nil
            if sCType == SQL_NUMERIC ||
               sCType == SQL_DECIMAL ||
               sCType == SQL_INTEGER ||
               sCType == SQL_SMALLINT ||
               sCType == SQL_USMALLINT ||
               sCType == SQL_UINTEGER ||
               sCType == SQL_UBIGINT ||
               sCType == SQL_BIGINT ||
               sCType == SQL_TINYINT {
                if C.getColLen(sMachbaseStmt.sColRLen, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = int64(C.getlonglongValue(sMachbaseStmt.sLongData, C.int(i)))
                }
            } else if sCType == SQL_FLOAT ||
                      sCType == SQL_REAL ||
                      sCType == SQL_DOUBLE {
                if C.getColLen(sMachbaseStmt.sColRLen, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = float64(C.getDoubleValue(sMachbaseStmt.sDoubleData, C.int(i)))
                }
            } else if sCType == SQL_BINARY {
                if C.getColLen(sMachbaseStmt.sColRLen, C.int(i)) != C.SQL_NULL_DATA {
                    sDecoded, _ := hex.DecodeString(C.GoString(C.getCharValue(sMachbaseStmt.sStrData, C.int(i))))
                    aInterfaceArr[i] = string(sDecoded)
                }
            } else {
                if C.getColLen(sMachbaseStmt.sColRLen, C.int(i)) != C.SQL_NULL_DATA {
                    aInterfaceArr[i] = C.GoString(C.getCharValue(sMachbaseStmt.sStrData, C.int(i)))
                }
            }
        }
        return RC_SUCCESS
    } else {
        return RC_FAILURE
    }
}

func (sMachbaseStmt *MachbaseStmt) PrintStmtErr() string {
    return C.GoString(sMachbaseStmt.sErrStmt)
}