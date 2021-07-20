package machbase

import (
    // "fmt"
    "time"
    "bytes"
    "errors"
    "strconv"
    "strings"
    "net/url"
    "net/http"
    "io/ioutil"
    "encoding/json"
    "encoding/base64"
)

type MachbaseConnect struct {
    Ip          string
    Port        string
    Id          string
    Pw          string
    Type        string
    Method      string
    Sql         string
    DateFormat  string
    Scale       int
    FetchMode   int
    TimeOut     int
}

type MachbaseAppend struct {
    Name        string           `json:"name"`
    Dateformat  string           `json:"date_format"`
    Value       [][]interface{}  `json:"values"`
}

type MachbaseResultAppend struct {
    ErrorCode     int     `json:"error_code"`
    ErrorMessage  string  `json:"error_message"`
    SuccessCount  int     `json:"append_success"`
    FailCount     int     `json:"append_failure"`
}

type MachbaseResult struct {
    ErrorCode     int                       `json:"error_code"`
    ErrorMessage  string                    `json:"error_message"`
    Columns       []MachbaseColumn          `json:"columns"`
    Data          []map[string]interface{}  `json:"data"`
}

type MachbaseResultV2 struct {
    ErrorCode     int               `json:"error_code"`
    ErrorMessage  string            `json:"error_message"`
    Columns       []MachbaseColumn  `json:"columns"`
    Data          [][]interface{}   `json:"data"`
}

type MachbaseColumn struct {
    Name    string  `json:"name"`
    Type    int     `json:"type"`
    Length  int     `json:"length"`
}

type SetOption func(*MachbaseConnect)

const (
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

    MACHBASE_GET   string = "GET"
    MACHBASE_POST  string = "POST"
    MACHBASE_CLOUD string = "CLOUD"
    MACHBASE_REST  string = "REST"

    MACHBASE_FETCHMODE_0 int = 0
    MACHBASE_FETCHMODE_1 int = 1

    MACHBASE_DEFAULT_DATEFORMAT string = "YYYY-MM-DD HH24:MI:SS mmm:uuu:nnn"

    MACHBASE_SCALE_0 int = 0
    MACHBASE_SCALE_1 int = 1
    MACHBASE_SCALE_2 int = 2
    MACHBASE_SCALE_3 int = 3
    MACHBASE_SCALE_4 int = 4
    MACHBASE_SCALE_5 int = 5
    MACHBASE_SCALE_6 int = 6
    MACHBASE_SCALE_7 int = 7
    MACHBASE_SCALE_8 int = 8
    MACHBASE_SCALE_9 int = 9
)

/*  Machbase 6.5 RestAPI Code  */
func CreateConnect(aOptions ...SetOption) *MachbaseConnect {
    return new(MachbaseConnect).SetConnectOption(aOptions...)
}

func (rConnect *MachbaseConnect) SetConnectOption(aOptions ...SetOption) *MachbaseConnect {
    for _, sFuntion := range aOptions {
        sFuntion(rConnect)
    }
    return rConnect
}

func Ip(aIp string) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.Ip = aIp
    }
}

func Port(aPort string) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.Port = aPort
    }
}

func Id(aId string) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.Id = aId
    }
}

func Pw(aPw string) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.Pw = aPw
    }
}

func Type(aType string) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.Type = aType
    }
}

func Method(aMethod string) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.Method = aMethod
    }
}

func Sql(aSql string) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.Sql = aSql
    }
}

func DateFormat(aDateFormat string) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.DateFormat = aDateFormat
    }
}

func Scale(aScale int) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.Scale = aScale
    }
}

func FetchMode(aFetchMode int) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.FetchMode = aFetchMode
    }
}

func TimeOut(aTimeOut int) SetOption {
    return func(aConnect *MachbaseConnect) {
        aConnect.TimeOut = aTimeOut
    }
}

func (rConnect *MachbaseConnect) MakeUrl() string {
    var sUrl string = ""

    if rConnect.Type == MACHBASE_REST {
        sUrl = "http://" + rConnect.Ip + ":" + rConnect.Port + "/machbase"
    } else {
        //Cloud Url
    }

    return sUrl
}

func (rConnect *MachbaseConnect) MakeQueryString(aUrl string) string {
    var sUrl string = aUrl

    if rConnect.Sql != "" {
        sUrl = sUrl + "?q=" + url.QueryEscape(rConnect.Sql)
    }

    if ((rConnect.Sql != "") && (rConnect.DateFormat != "")) {
        sUrl = sUrl + "&f=" + url.QueryEscape(rConnect.DateFormat)
    }

    if ((rConnect.Sql != "") && (rConnect.Scale != 0)) {
        sUrl = sUrl + "&s=" + strconv.Itoa(rConnect.Scale)
    }

    if ((rConnect.Sql != "") && (rConnect.FetchMode != 0)) {
        sUrl = sUrl + "&m=" + strconv.Itoa(rConnect.FetchMode)
    }

    return sUrl
}

func (rConnect *MachbaseConnect) MakeBasicAuth() string {
    var sKey string = rConnect.Id + "@" + rConnect.Ip + ":" + rConnect.Pw

    sKey = base64.StdEncoding.EncodeToString([]byte(sKey))

    return sKey
}

func MakeAppend(aTableName, aDateFormat string, aData [][]interface{}) (*bytes.Buffer, error) {
    var (
        sAppendData MachbaseAppend = MachbaseAppend{}
        sError      error          = nil
        sJsonBytes  []byte         = nil
        sBuffer     *bytes.Buffer  = nil
    )

    defer func() {
        sError = nil
        sJsonBytes = nil
        sBuffer = nil
    }()

    sAppendData.Name = aTableName
    sAppendData.Dateformat = aDateFormat
    sAppendData.Value = aData

    sJsonBytes, sError = json.Marshal(sAppendData)
    if sError != nil {
        return nil, sError
    }

    sBuffer = bytes.NewBuffer(sJsonBytes)

    return sBuffer, nil
}

func (rConnect *MachbaseConnect) SendRequest(aData *bytes.Buffer, aKey2 string) ([]byte, error) {
    var (
        sUrl      string         = ""
        sKey      string         = ""
        sRequest  *http.Request  = nil
        sError    error          = nil
        sClient   *http.Client   = &http.Client{}
        sResponse *http.Response = nil
        sBytes    []byte         = nil
    )

    defer func() {
        if sResponse != nil {
            sResponse.Body.Close()
        }

        sRequest = nil
        sError = nil
        sClient = nil
        sResponse = nil
        sBytes = nil
    }()

    sUrl = rConnect.MakeUrl()
    sUrl = rConnect.MakeQueryString(sUrl)
    sKey = rConnect.MakeBasicAuth()

    if rConnect.Method == MACHBASE_GET {
        sRequest, sError = http.NewRequest(rConnect.Method, sUrl, nil)
    } else {
        sRequest, sError = http.NewRequest(rConnect.Method, sUrl, aData)
    }

    if sError != nil {
        return nil, sError
    }

    sRequest.Header.Add("Authorization","Basic " + sKey)

    if aKey2 == "" {
        aKey2 = ""
    }

    if rConnect.Method == MACHBASE_POST {
        // Add Content-Type Header
        sRequest.Header.Add("Content-Type", "application/json")
    }

    if rConnect.TimeOut != 0 {
        sClient.Timeout = time.Duration(rConnect.TimeOut) * time.Second
    } else {
        // Nothing To Do
    }

    sResponse, sError = sClient.Do(sRequest)
    if sError != nil {
        return nil, sError
    }

    sBytes, sError = ioutil.ReadAll(sResponse.Body)
    if sError != nil {
        return nil, sError
    }

    if len(sBytes) < 2 {
        return nil, errors.New("The JSON Format is not valid")
    }

    if ((sBytes[0] == 0x7B || sBytes[1] == 0x7B) && (sBytes[len(sBytes)-1] == 0x7D || sBytes[len(sBytes)-2] == 0x7D)) {
        return sBytes, nil
    } else {
        return nil, errors.New("The JSON Format is not valid")
    }
}

func MakeResultAppend(aData []byte) (MachbaseResultAppend, error) {
    var (
        sResultData MachbaseResultAppend = MachbaseResultAppend{}
        sError      error                = nil
    )

    defer func() {
        sError = nil
    }()

    if sError = json.Unmarshal(aData, &sResultData); sError != nil {
        return sResultData, sError
    }

    return sResultData, nil
}

func MakeResult(aData []byte) (MachbaseResult, error) {
    var (
        sResultData MachbaseResult = MachbaseResult{}
        sError      error          = nil
    )

    defer func() {
        sError = nil
    }()

    if sError = json.Unmarshal(aData, &sResultData); sError != nil {
        return sResultData, sError
    }

    return sResultData, nil
}

func MakeResultV2(aData []byte) (MachbaseResultV2, error) {
    var (
        sResultData MachbaseResultV2 = MachbaseResultV2{}
        sError      error            = nil
    )

    defer func() {
        sError = nil
    }()

    if sError = json.Unmarshal(aData, &sResultData); sError != nil {
        return sResultData, sError
    }

    return sResultData, nil
}

func MakeColumnNameList(aColumns []MachbaseColumn) []string {
    var sList []string = make([]string, len(aColumns))

    defer func() {
        sList = nil
    }()

    for sIdx, sValue := range aColumns {
        sList[sIdx] = sValue.Name
    }

    return sList
}

func MakeColumnTypeList(aColumns []MachbaseColumn) []int {
    var sList []int = make([]int, len(aColumns))

    defer func() {
        sList = nil
    }()

    for sIdx, sValue := range aColumns {
        sList[sIdx] = sValue.Type
    }

    return sList
}

func MakeColumnLengthList(aColumns []MachbaseColumn) []int {
    var sList []int = make([]int, len(aColumns))

    defer func() {
        sList = nil
    }()

    for sIdx, sValue := range aColumns {
        sList[sIdx] = sValue.Length
    }

    return sList
}

func ConversionType(aType string) int {
    var (
        sType   string = strings.ToUpper(strings.TrimSpace(aType))
        sResult int    = -1
    )

    switch sType {
        case "SHORT":
            sResult = MACHBASE_SHORT
        case "INTEGER":
            sResult = MACHBASE_INTEGER
        case "LONG":
            sResult = MACHBASE_LONG
        case "FLOAT":
            sResult = MACHBASE_FLOAT
        case "DOUBLE":
            sResult = MACHBASE_DOUBLE
        case "VARCHAR":
            sResult = MACHBASE_VARCHAR
        case "IPV4":
            sResult = MACHBASE_IPV4
        case "IPV6":
            sResult = MACHBASE_IPV6
        case "DATETIME":
            sResult = MACHBASE_DATETIME
        case "TEXT":
            sResult = MACHBASE_TEXT
        case "BINARY":
            sResult = MACHBASE_BINARY
        case "USHORT":
            sResult = MACHBASE_USHORT
        case "UINTEGER":
            sResult = MACHBASE_UINTEGER
        case "ULONG":
            sResult = MACHBASE_ULONG
        default:
            sResult = -1
    }

    return sResult
}

func CheckType(aType int) string {
    if (aType == 4) ||
       (aType == 8) ||
       (aType == 12) {
        return "int64"
    } else if (aType == 16) ||
              (aType == 20) {
        return "float64"
    } else if (aType == 97) ||
              (aType == 57) ||
              (aType == 53) {
        return "[]byte"
    } else if (aType == 104) ||
              (aType == 108) ||
              (aType == 112) {
        return "uint64"
    } else {
        return "string"
    }
}