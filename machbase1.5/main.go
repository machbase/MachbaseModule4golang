package main

import (
    "os"
    "fmt"
    "time"
    "strconv"
    "io/ioutil"
    "./machbase"
)

const (
    RC_SUCCESS int = 0
    RC_FAILURE int = -1
)

var (
    ip     string = "127.0.0.1"
    port   string = "5656"
    id     string = "SYS"
    pw     string = "MANAGER"
    driver string = fmt.Sprintf("SERVER=%s;UID=%s;PWD=%s;CONNTYPE=1;PORT_NO=%s;CONNECTION_TIMEOUT=3;TIMEZONE=+0900", ip, id, pw, port)
)

func main() {
    num := 0

    for {
        fmt.Println("*****************************************************************")
        fmt.Println("* 01.ConnectMachbase                                            *")
        fmt.Println("* 02.CreateStatement                                            *")
        fmt.Println("* 03.CreateTable                                                *")
        fmt.Println("* 04.DropTable                                                  *")
        fmt.Println("* 05.Append(String)                                             *")
        fmt.Println("* 06.Append(Interface)                                          *")
        fmt.Println("* 07.Select                                                     *")
        fmt.Println("* 08.SelectMap                                                  *")
        fmt.Println("* 09.SelectSchema                                               *")
        fmt.Println("* 10.Image(String)                                              *")
        fmt.Println("* 11.Image(Interface)                                           *")
        fmt.Println("* 12.Exit                                                       *")
        fmt.Println("*****************************************************************")
        fmt.Print("Please enter a number : ")
        fmt.Scan(&num)

        if num == 1 {
            ConnectMachbase()
        } else if num == 2 {
            CreateStatement()
        } else if num == 3 {
            DropTable()
            CreateTable()
        } else if num == 4 {
            DropTable()
        } else if num == 5 {
            DropTable()
            CreateTable()
            AppendString()
        } else if num == 6 {
            DropTable()
            CreateTable()
            AppendInterface()
        } else if num == 7 {
            DropTable()
            CreateTable()
            AppendInterface()
            Select()
        } else if num == 8 {
            DropTable()
            CreateTable()
            AppendInterface()
            SelectMap()
        } else if num == 9 {
            DropTable()
            CreateTable()
            AppendInterface()
            SelectSchema()
        } else if num == 10 {
            DropTable()
            CreateTable()
            ImageAppendString()
            ImageSelect()
        } else if num == 11 {
            DropTable()
            CreateTable()
            ImageAppendInterface()
            ImageSelect()
        } else {
            break
        }
    }
}

func ConnectMachbase() {
    // create connect struct
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    // db conenct
    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
    } else {
        fmt.Println("Machbase Connect Success!!")
    }
}

func CreateStatement() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    // create stmt
    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    // stmt alloc
    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
    } else {
        fmt.Println("Machbase Statement Alloc Success!!")
    }
}

func CreateTable() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    sql := "CREATE TABLE GO_SAMPLE(IDX INTEGER, D1 SHORT, D2 INTEGER, D3 LONG, F1 FLOAT, F2 DOUBLE, NAME VARCHAR(20), TEXT TEXT, IMAGE BINARY, V4 IPV4, V6 IPV6, DT DATETIME)"

    // create sample table
    if stmt.ExecDirect(sql) == RC_SUCCESS {
        fmt.Println("Create Sample Table Success!!")
    } else {
        fmt.Println("Create Sample Table Fail : ", stmt.PrintStmtErr())
    }
}

func DropTable() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    sql := "DROP TABLE GO_SAMPLE"

    // drop sample table
    if stmt.ExecDirect(sql) == RC_SUCCESS {
        fmt.Println("Drop Sample Table Success!!")
    } else {
        fmt.Println("Drop Sample Table Fail : ", stmt.PrintStmtErr())
    }
}

func AppendString() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    // data append open
    table := "GO_SAMPLE"
    if stmt.AppendOpen(table) != RC_SUCCESS {
        fmt.Println("AppendOpen Fail : ", stmt.PrintStmtErr())
        return
    }

    length := 12
    count := 0
    columnType := make([]int, length)
    value := make([]string, length)
    dateformat := ""
    // dateformat := "YYYY-MM-DD HH24:MI:SS mmm:uuu:nnn"  //use timeformat

    //data append start
    for count < (length + 1) {
        columnType[0] = machbase.MACHBASE_INTEGER
        value[0] = strconv.Itoa(count)

        columnType[1] = machbase.MACHBASE_SHORT
        value[1] = strconv.Itoa(count)

        columnType[2] = machbase.MACHBASE_INTEGER
        value[2] = strconv.Itoa(count + count)

        columnType[3] = machbase.MACHBASE_LONG
        value[3] = strconv.Itoa((count + count) * 100)

        columnType[4] = machbase.MACHBASE_FLOAT
        value[4] = strconv.Itoa(count)

        columnType[5] = machbase.MACHBASE_DOUBLE
        value[5] = strconv.Itoa(count)

        columnType[6] = machbase.MACHBASE_VARCHAR
        value[6] = "Varchar Test" + strconv.Itoa(count)

        columnType[7] = machbase.MACHBASE_TEXT
        value[7] = "Text Test" + strconv.Itoa(count)

        columnType[8] = machbase.MACHBASE_BINARY
        value[8] = "Binary Test" + strconv.Itoa(count)

        columnType[9] = machbase.MACHBASE_IPV4
        value[9] = "192.168.0." + strconv.Itoa(count)

        columnType[10] = machbase.MACHBASE_IPV6
        value[10] = fmt.Sprintf("2001:0DB8:0000:0000:0000:0000:1428:%04d", count)

        columnType[11] = machbase.MACHBASE_DATETIME
        value[11] = strconv.FormatInt(time.Now().UnixNano(), 10)
        // value[11] = time.Now().Format("2006-01-02 15:04:05") + " 000:000:000"  //use timeformat

        for i := count; i < length; i++ {
            value[i] = ""
        }

        if stmt.AppendDataV2(columnType, value, dateformat, length) != RC_SUCCESS {
            fmt.Println("AppendDataV2 Fail : ", stmt.PrintStmtErr())
        }

        count++
    }

    if stmt.AppendFlush() != RC_SUCCESS {
        fmt.Println("AppendFlush Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("AppendFlush Success!!")
    }

    // append finish
    result := stmt.AppendClose()
    if result == RC_FAILURE {
        fmt.Println("AppendClose Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Success Count : ", result)
    }
}

func AppendInterface() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    // data append open
    table := "GO_SAMPLE"
    if stmt.AppendOpen(table) != RC_SUCCESS {
        fmt.Println("AppendOpen Fail : ", stmt.PrintStmtErr())
        return
    }

    length := 12
    count := 0
    columnType := make([]int, length)
    value := make([]interface{}, length)
    dateformat := ""
    // dateformat := "YYYY-MM-DD HH24:MI:SS mmm:uuu:nnn"  //use timeformat

    //data append start
    for count < (length + 1) {
        columnType[0] = machbase.MACHBASE_INTEGER
        value[0] = int64(count)

        columnType[1] = machbase.MACHBASE_SHORT
        value[1] = int64(count)

        columnType[2] = machbase.MACHBASE_INTEGER
        value[2] = int64(count + count)

        columnType[3] = machbase.MACHBASE_LONG
        value[3] = int64((count + count) * 100)

        columnType[4] = machbase.MACHBASE_FLOAT
        value[4] = float64(count)

        columnType[5] = machbase.MACHBASE_DOUBLE
        value[5] = float64(count)

        columnType[6] = machbase.MACHBASE_VARCHAR
        value[6] = "Varchar Test" + strconv.Itoa(count)

        columnType[7] = machbase.MACHBASE_TEXT
        value[7] = "Text Test" + strconv.Itoa(count)

        columnType[8] = machbase.MACHBASE_BINARY
        value[8] = []byte("Binary Test" + strconv.Itoa(count))

        columnType[9] = machbase.MACHBASE_IPV4
        value[9] = "192.168.0." + strconv.Itoa(count)

        columnType[10] = machbase.MACHBASE_IPV6
        value[10] = fmt.Sprintf("2001:0DB8:0000:0000:0000:0000:1428:%04d", count)

        columnType[11] = machbase.MACHBASE_DATETIME
        value[11] = time.Now().UnixNano()
        // value[11] = time.Now().Format("2006-01-02 15:04:05") + " 000:000:000"  //use timeformat

        for i := count; i < length; i++ {
            value[i] = nil
        }

        if stmt.AppendDataV2I(columnType, value, dateformat, length) != RC_SUCCESS {
            fmt.Println("AppendDataV2I Fail : ", stmt.PrintStmtErr())
        }

        count++
    }

    if stmt.AppendFlush() != RC_SUCCESS {
        fmt.Println("AppendFlush Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("AppendFlush Success!!")
    }

    // append finish
    result := stmt.AppendClose()
    if result == RC_FAILURE {
        fmt.Println("AppendClose Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Success Count : ", result)
    }
}

func Select() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    sql := "SELECT * FROM GO_SAMPLE"
    if stmt.Prepare(sql) != RC_SUCCESS {
        fmt.Println("Prepare Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Prepare Success!!")
    }

    if stmt.Execute() != RC_SUCCESS {
        fmt.Println("Execute Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Execute Success!!")
    }

    dataList := make([]interface{}, stmt.GetColCount())
    for {
        if stmt.Fetch(dataList) == RC_FAILURE {
            fmt.Println("Fetch Fail : ", stmt.PrintStmtErr())
            break
        }

        fmt.Println("IDX : ", dataList[0])
        fmt.Println("D1 : ", dataList[1])
        fmt.Println("D2 : ", dataList[2])
        fmt.Println("D3 : ", dataList[3])
        fmt.Println("F1 : ", dataList[4])
        fmt.Println("F2 : ", dataList[5])
        fmt.Println("NAME : ", dataList[6])
        fmt.Println("TEXT : ", dataList[7])
        if dataList[8] != nil {
            fmt.Println("IMAGE : ", string(dataList[8].([]byte)))
        } else {
            fmt.Println("IMAGE : ", dataList[8])
        }
        fmt.Println("IPV4 : ", dataList[9])
        fmt.Println("IPV6 : ", dataList[10])
        fmt.Println("TIME : ", dataList[11])
        fmt.Println("*****************************************")
    }
}

func SelectMap() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    sql := "SELECT * FROM GO_SAMPLE"
    if stmt.Prepare(sql) != RC_SUCCESS {
        fmt.Println("Prepare Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Prepare Success!!")
    }

    if stmt.Execute() != RC_SUCCESS {
        fmt.Println("Execute Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Execute Success!!")
    }

    dataMap := make(map[string]interface{}, stmt.GetColCount())
    for {
        if stmt.FetchMap(dataMap) == RC_FAILURE {
            fmt.Println("Fetch Fail : ",stmt.PrintStmtErr())
            break
        }

        fmt.Println("IDX : ", dataMap["IDX"])
        fmt.Println("D1 : ", dataMap["D1"])
        fmt.Println("D2 : ", dataMap["D2"])
        fmt.Println("D3 : ", dataMap["D3"])
        fmt.Println("F1 : ", dataMap["F1"])
        fmt.Println("F2 : ", dataMap["F2"])
        fmt.Println("NAME : ", dataMap["NAME"])
        fmt.Println("TEXT : ", dataMap["TEXT"])
        if dataMap["IMAGE"] != nil {
            fmt.Println("IMAGE : ", string(dataMap["IMAGE"].([]byte)))
        } else {
            fmt.Println("IMAGE : ", dataMap["IMAGE"])
        }
        fmt.Println("IPV4 : ", dataMap["V4"])
        fmt.Println("IPV6 : ", dataMap["V6"])
        fmt.Println("TIME : ", dataMap["DT"])
        fmt.Println("*****************************************")
    }
}

func SelectSchema() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    sql := "SELECT * FROM GO_SAMPLE"
    if stmt.Prepare(sql) != RC_SUCCESS {
        fmt.Println("Prepare Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Prepare Success!!")
    }

    if stmt.Execute() != RC_SUCCESS {
        fmt.Println("Execute Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Execute Success!!")
    }

    schemaList, _ := stmt.Schema()
    for i := 0; i < len(schemaList); i++ {
        fmt.Println("Schema Name : ", schemaList[i].Name)
        fmt.Println("Schema SqlType : ", schemaList[i].SqlType)
        fmt.Println("Schema ColType : ", schemaList[i].ColType)
        fmt.Println("Schema Length : ", schemaList[i].Length)
        fmt.Println("*****************************************")
    }
}

func ImageAppendString() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    // data append open
    table := "GO_SAMPLE"
    if stmt.AppendOpen(table) != RC_SUCCESS {
        fmt.Println("AppendOpen Fail : ", stmt.PrintStmtErr())
        return
    }

    length := 12
    columnType := make([]int, length)
    value := make([]string, length)
    dateformat := ""
    // dateformat := "YYYY-MM-DD HH24:MI:SS mmm:uuu:nnn"  //use timeformat

    columnType[0] = machbase.MACHBASE_INTEGER
    value[0] = ""

    columnType[1] = machbase.MACHBASE_SHORT
    value[1] = ""

    columnType[2] = machbase.MACHBASE_INTEGER
    value[2] = ""

    columnType[3] = machbase.MACHBASE_LONG
    value[3] = ""

    columnType[4] = machbase.MACHBASE_FLOAT
    value[4] = ""

    columnType[5] = machbase.MACHBASE_DOUBLE
    value[5] = ""

    columnType[6] = machbase.MACHBASE_VARCHAR
    value[6] = ""

    columnType[7] = machbase.MACHBASE_TEXT
    value[7] = ""

    columnType[8] = machbase.MACHBASE_BINARY
    value[8] = string(readFile())

    columnType[9] = machbase.MACHBASE_IPV4
    value[9] = ""

    columnType[10] = machbase.MACHBASE_IPV6
    value[10] = ""

    columnType[11] = machbase.MACHBASE_DATETIME
    value[11] = ""

    if stmt.AppendDataV2(columnType, value, dateformat, length) != RC_SUCCESS {
        fmt.Println("AppendDataV2 Fail : ", stmt.PrintStmtErr())
    }

    if stmt.AppendFlush() != RC_SUCCESS {
        fmt.Println("AppendFlush Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("AppendFlush Success!!")
    }

    result := stmt.AppendClose()
    if result == RC_FAILURE {
        fmt.Println("AppendClose Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Success Count : ",result)
    }
}

func ImageAppendInterface() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    // data append open
    table := "GO_SAMPLE"
    if stmt.AppendOpen(table) != RC_SUCCESS {
        fmt.Println("AppendOpen Fail : ", stmt.PrintStmtErr())
        return
    }

    length := 12
    columnType := make([]int, length)
    value := make([]interface{}, length)
    dateformat := ""
    // dateformat := "YYYY-MM-DD HH24:MI:SS mmm:uuu:nnn"  //use timeformat

    columnType[0] = machbase.MACHBASE_INTEGER
    value[0] = nil

    columnType[1] = machbase.MACHBASE_SHORT
    value[1] = nil

    columnType[2] = machbase.MACHBASE_INTEGER
    value[2] = nil

    columnType[3] = machbase.MACHBASE_LONG
    value[3] = nil

    columnType[4] = machbase.MACHBASE_FLOAT
    value[4] = nil

    columnType[5] = machbase.MACHBASE_DOUBLE
    value[5] = nil

    columnType[6] = machbase.MACHBASE_VARCHAR
    value[6] = nil

    columnType[7] = machbase.MACHBASE_TEXT
    value[7] = nil

    columnType[8] = machbase.MACHBASE_BINARY
    value[8] = readFile()

    columnType[9] = machbase.MACHBASE_IPV4
    value[9] = nil

    columnType[10] = machbase.MACHBASE_IPV6
    value[10] = nil

    columnType[11] = machbase.MACHBASE_DATETIME
    value[11] = nil

    if stmt.AppendDataV2I(columnType, value, dateformat, length) != RC_SUCCESS {
        fmt.Println("AppendDataV2I Fail : ", stmt.PrintStmtErr())
    }

    if stmt.AppendFlush() != RC_SUCCESS {
        fmt.Println("AppendFlush Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("AppendFlush Success!!")
    }

    result := stmt.AppendClose()
    if result == RC_FAILURE {
        fmt.Println("AppendClose Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Success Count : ",result)
    }
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

func ImageSelect() {
    connect := machbase.CreateConnect()
    defer connect.DisconnectDB()

    if connect.ConnectDB(driver) == RC_FAILURE {
        fmt.Println(connect.PrintConErr())
        return
    }

    stmt := connect.CreateStmt()
    defer stmt.FreeStmt()

    if stmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(stmt.PrintStmtErr())
        return
    }

    sql := "SELECT IMAGE FROM GO_SAMPLE"
    if stmt.Prepare(sql) != RC_SUCCESS {
        fmt.Println("Prepare Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Prepare Success!!")
    }

    if stmt.Execute() != RC_SUCCESS {
        fmt.Println("Execute Fail : ", stmt.PrintStmtErr())
    } else {
        fmt.Println("Execute Success!!")
    }

    dataList := make([]interface{}, stmt.GetColCount())
    if stmt.Fetch(dataList) == RC_FAILURE {
        fmt.Println("Fetch Fail : ", stmt.PrintStmtErr())
    } else {
        saveFile(dataList[0].([]byte))
    }
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