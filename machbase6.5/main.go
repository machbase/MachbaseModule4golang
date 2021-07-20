package main

import (
    "os"
    "fmt"
    "time"
    "strconv"
    "io/ioutil"
    "./machbase"
    "encoding/base64"
)

var (
    ip   string = "127.0.0.1"
    port string = "5657"
    id   string = "SYS"
    pw   string = "MANAGER"
)

func main() {
    DeleteTable()
    CreateTable()
    AppendRestAPI()
    SelectRestAPI()
    SelectRestAPIOption()
}

func CreateTable() {
    connect := machbase.CreateConnect(
        machbase.Ip(ip),
        machbase.Port(port),
        machbase.Id(id),
        machbase.Pw(pw),
        machbase.Type(machbase.MACHBASE_REST),
    )

    sql := "CREATE TABLE GO_SAMPLE(IDX INTEGER, D1 SHORT, D2 INTEGER, D3 LONG, F1 FLOAT, F2 DOUBLE, NAME VARCHAR(80), TEXT TEXT, IMAGE BINARY, IP4 IPV4, IP6 IPV6, TIME DATETIME)"
    connect = connect.SetConnectOption(
        machbase.Sql(sql),
        machbase.Method(machbase.MACHBASE_GET),
        machbase.DateFormat(machbase.MACHBASE_DEFAULT_DATEFORMAT),
        machbase.Scale(machbase.MACHBASE_SCALE_0),
        machbase.FetchMode(machbase.MACHBASE_FETCHMODE_0),
    )

    result, err := connect.SendRequest(nil, "")
    if err != nil {
        fmt.Println("Send Request Error : ", err)
    }

    data, err := machbase.MakeResult(result)
    if err != nil {
        fmt.Println("Select Json Unmarshal Fail : ", err)
    }

    fmt.Println("---------------------------------------------CreateRestAPI---------------------------------------------")
    fmt.Println("Select ErrorCode : ", data.ErrorCode)
    fmt.Println("Select ErrorMessage : ", data.ErrorMessage)
    fmt.Println("Select Columns : ", data.Columns)
    fmt.Println("Select Data : ", data.Data)
    fmt.Println("---------------------------------------------CreateRestAPI---------------------------------------------")
}

func DeleteTable() {
    sql := "DROP TABLE GO_SAMPLE"

    connect := machbase.CreateConnect(
        machbase.Ip(ip),
        machbase.Port(port),
        machbase.Id(id),
        machbase.Pw(pw),
        machbase.Type(machbase.MACHBASE_REST),
        machbase.Sql(sql),
        machbase.Method(machbase.MACHBASE_GET),
        machbase.DateFormat(machbase.MACHBASE_DEFAULT_DATEFORMAT),
        machbase.Scale(machbase.MACHBASE_SCALE_0),
        machbase.FetchMode(machbase.MACHBASE_FETCHMODE_0),
    )

    result, err := connect.SendRequest(nil, "")
    if err != nil {
        fmt.Println("Send Request Error : ", err)
    }

    data, err := machbase.MakeResult(result)
    if err != nil {
        fmt.Println("Select Json Unmarshal Fail : ", err)
    }

    fmt.Println("---------------------------------------------DropRestAPI---------------------------------------------")
    fmt.Println("Select ErrorCode : ", data.ErrorCode)
    fmt.Println("Select ErrorMessage : ", data.ErrorMessage)
    fmt.Println("Select Columns : ", data.Columns)
    fmt.Println("Select Data : ", data.Data)
    fmt.Println("---------------------------------------------DropRestAPI---------------------------------------------")
}

func AppendRestAPI() {
    columnCount := 12
    rowCount := 13
    dataSet := make([][]interface{}, rowCount)
    dataRow := make([]interface{}, columnCount)

    for i := 0; i < rowCount; i++ {
        dataSet[i] =  make([]interface{}, columnCount)

        dataRow[0] = strconv.Itoa(i)	                          //integer
        dataRow[1] = i	                                          //short
        dataRow[2] = i + i	                                      //integer
        dataRow[3] = (i + i) * 100	                              //long
        dataRow[4] = float32(i+1) / (float32(i+2) * float32(2))	  //float32
        dataRow[5] = float64(i+1) / (float64(i+2) * float64(2))	  //float64
        dataRow[6] = "varchar" + strconv.Itoa(i)	              //varchar
        dataRow[7] = "text" + strconv.Itoa(i)	                  //text
        dataRow[8] = []byte("binary" + strconv.Itoa(i))           //binary
        dataRow[9] = "192.168.0." + strconv.Itoa(i)	              //ipv4
        dataRow[10] = "2001:0DB8:1000:0000:0000:0000:1111:2222"   //ipv6
        dataRow[11] = time.Now().UnixNano()                   	  //datetime

        for j := i; j < columnCount; j++ {
            dataRow[j] = nil
        }

        if (i == (rowCount - 1)) {
            dataRow[8] = readFile()
        }

        copy(dataSet[i], dataRow)
    }

    data, err := machbase.MakeAppend("GO_SAMPLE", "", dataSet)
    if err != nil {
        fmt.Println("Append Json Marshal Fail : ", err)
    }

    connect := machbase.CreateConnect(
        machbase.Ip(ip),
        machbase.Port(port),
        machbase.Id(id),
        machbase.Pw(pw),
        machbase.Type(machbase.MACHBASE_REST),
        machbase.Sql(""),
        machbase.Method(machbase.MACHBASE_POST),
        machbase.DateFormat(""),
        machbase.Scale(machbase.MACHBASE_SCALE_0),
        machbase.FetchMode(machbase.MACHBASE_FETCHMODE_0),
    )

    result, err := connect.SendRequest(data, "")
    if err != nil {
        fmt.Println("Send Request Error : ", err)
    }

    appendResult, err := machbase.MakeResultAppend(result)
    if err != nil {
        fmt.Println("Append Result Json Unmarshal Fail : ", err)
    }

    fmt.Println("---------------------------------------------AppendRestAPI---------------------------------------------")
    fmt.Println("Error Code : ", appendResult.ErrorCode)
    fmt.Println("Error Message : ", appendResult.ErrorMessage)
    fmt.Println("Append SuccessCount : ", appendResult.SuccessCount)
    fmt.Println("Append FailCount : ", appendResult.FailCount)
    fmt.Println("---------------------------------------------AppendRestAPI---------------------------------------------")
}

func readFile() []byte {
    path, err := os.Getwd()
    if err != nil {
        fmt.Println("Folder Path Error : ",err)
    }

    path = path + "/Machbase.png"

    image, err := ioutil.ReadFile(path)
    if err != nil {
        fmt.Println("readFile Error : ",err)
    }

    return image
}

func SelectRestAPI() {
    sql := "SELECT * FROM GO_SAMPLE"

    connect := machbase.CreateConnect(
        machbase.Ip(ip),
        machbase.Port(port),
        machbase.Id(id),
        machbase.Pw(pw),
        machbase.Type(machbase.MACHBASE_REST),
        machbase.Sql(sql),
        machbase.Method(machbase.MACHBASE_GET),
        machbase.DateFormat(machbase.MACHBASE_DEFAULT_DATEFORMAT),
        machbase.Scale(machbase.MACHBASE_SCALE_5),
        machbase.FetchMode(machbase.MACHBASE_FETCHMODE_0),
    )

    result, err := connect.SendRequest(nil, "")
    if err != nil {
        fmt.Println("Send Request Error : ", err)
    }

    data, err := machbase.MakeResult(result)
    if err != nil {
        fmt.Println("Select Json Unmarshal Fail : ", err)
    }

    fmt.Println("----------------------------------------SelectRestAPIFetchMode0----------------------------------------")
    fmt.Println("Select ErrorCode : ", data.ErrorCode)
    fmt.Println("Select ErrorMessage : ", data.ErrorMessage)

    for i := 0; i < len(data.Columns); i++ {
        fmt.Println("Column Name : ", data.Columns[i].Name)
        fmt.Println("Column Type : ", data.Columns[i].Type)
        fmt.Println("Column Length : ", data.Columns[i].Length)
        fmt.Println("-----------------------------------------------")
    }

    columnList := machbase.MakeColumnNameList(data.Columns)

    for i := 0; i < len(data.Data); i++ {
        fmt.Println("Idx : ", data.Data[i][columnList[0]])
        fmt.Println("D1 : ", data.Data[i][columnList[1]])
        fmt.Println("D2 : ", data.Data[i][columnList[2]])
        fmt.Println("D3 : ", data.Data[i][columnList[3]])
        fmt.Println("F1 : ", data.Data[i][columnList[4]])
        fmt.Println("F2 : ", data.Data[i][columnList[5]])
        fmt.Println("VARCHAR : ", data.Data[i][columnList[6]])
        fmt.Println("TEXT : ", data.Data[i][columnList[7]])
        fmt.Println("BINARY : ", data.Data[i][columnList[8]])
        fmt.Println("IPV4 : ", data.Data[i][columnList[9]])
        fmt.Println("IPV6 : ", data.Data[i][columnList[10]])
        fmt.Println("DATETIME : ", data.Data[i][columnList[11]])
        fmt.Println("-----------------------------------------------")

        if i == 0 {
            sDecode, _ := base64.StdEncoding.DecodeString(data.Data[i][columnList[8]].(string))
            saveFile(sDecode)
        }
    }
    fmt.Println("----------------------------------------SelectRestAPIFetchMode0----------------------------------------")
}

func SelectRestAPIOption() {
    sql := "SELECT * FROM GO_SAMPLE"

    connect := machbase.CreateConnect(
        machbase.Ip(ip),
        machbase.Port(port),
        machbase.Id(id),
        machbase.Pw(pw),
        machbase.Type(machbase.MACHBASE_REST),
        machbase.Sql(sql),
        machbase.Method(machbase.MACHBASE_GET),
        machbase.DateFormat(machbase.MACHBASE_DEFAULT_DATEFORMAT),
        machbase.Scale(machbase.MACHBASE_SCALE_9),
        machbase.FetchMode(machbase.MACHBASE_FETCHMODE_1),
    )

    result, err := connect.SendRequest(nil, "")
    if err != nil {
        fmt.Println("Send Request Error : ", err)
    }

    data, err := machbase.MakeResultV2(result)
    if err != nil {
        fmt.Println("Select Json Unmarshal Fail : ", err)
    }

    fmt.Println("----------------------------------------SelectRestAPIFetchMode1----------------------------------------")
    fmt.Println("Select ErrorCode : ", data.ErrorCode)
    fmt.Println("Select ErrorMessage : ", data.ErrorMessage)

    for i := 0; i < len(data.Columns); i++ {
        fmt.Println("Column Name : ", data.Columns[i].Name)
        fmt.Println("Column Type : ", data.Columns[i].Type)
        fmt.Println("Column Length : ", data.Columns[i].Length)
        fmt.Println("-----------------------------------------------")
    }

    for i := 0; i < len(data.Data); i++ {
        fmt.Println("Idx : ", data.Data[i][0])
        fmt.Println("D1 : ", data.Data[i][1])
        fmt.Println("D2 : ", data.Data[i][2])
        fmt.Println("D3 : ", data.Data[i][3])
        fmt.Println("F1 : ", data.Data[i][4])
        fmt.Println("F2 : ", data.Data[i][5])
        fmt.Println("VARCHAR : ", data.Data[i][6])
        fmt.Println("TEXT : ", data.Data[i][7])
        fmt.Println("BINARY : ", data.Data[i][8])
        fmt.Println("IPV4 : ", data.Data[i][9])
        fmt.Println("IPV6 : ", data.Data[i][10])
        fmt.Println("DATETIME : ", data.Data[i][11])
        fmt.Println("-----------------------------------------------")

        if i == 0 {
            sDecode, _ := base64.StdEncoding.DecodeString(data.Data[i][8].(string))
            saveFile(sDecode)
        }
    }
    fmt.Println("----------------------------------------SelectRestAPIFetchMode1----------------------------------------")
}

func saveFile(data []byte) {
    path, err := os.Getwd()
    if err != nil {
        fmt.Println("Folder Path Error : ",err)
    }

    path = path + "/Machbase2.png"

    file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0664))
    if err != nil {
        fmt.Println("error:", err)
    }

    _, err = file.Write(data)
    if err != nil {
        fmt.Println("error:", err)
    }
}