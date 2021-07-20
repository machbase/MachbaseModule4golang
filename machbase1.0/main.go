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

var RC_SUCCESS int = 0
var RC_FAILURE int = -1

func main() {
    var sNum int = 0

    for {
        fmt.Println("*****************************************************************")
        fmt.Println("* 01.ConnectMachbase                                            *")
        fmt.Println("* 02.CreateStatement                                            *")
        fmt.Println("* 03.CreateTable                                                *")
        fmt.Println("* 04.DropTable                                                  *")
        fmt.Println("* 05.Append                                                     *")
        fmt.Println("* 06.Select                                                     *")
        fmt.Println("* 07.Image                                                      *")
        fmt.Println("* 08.Exit                                                       *")
        fmt.Println("*****************************************************************")
        fmt.Print("Please enter a number : ")
        fmt.Scan(&sNum)

        if sNum == 1 {
            ConnectMachbase()
        } else if sNum == 2 {
            CreateStatement()
        } else if sNum == 3 {
            DropTable()
            CreateTable()
        } else if sNum == 4 {
            DropTable()
        } else if sNum == 5 {
            DropTable()
            CreateTable()
            Append()
        } else if sNum == 6 {
            DropTable()
            CreateTable()
            Append()
            Select()
        } else if sNum == 7 {
            DropTable()
            CreateTable()
            ImageAppend()
            ImageSelect()
        } else {
            break
        }
    }
}

func ConnectMachbase() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sIp string = "127.0.0.1"
    var sPort string = "5656"
    var sId string = "SYS"
    var sPass string = "MANAGER"
    var sDriver string = ""

    // create connect struct
    sMachbaseConnect = machbase.CreateConnect()

    defer func() {
        if sMachbaseConnect.DisconnectDB() == RC_FAILURE {
            fmt.Println("Machbase DisConnect Fail")
        } else {
            fmt.Println("Machbase DisConnect Success")
        }
        sMachbaseConnect = nil
    }()

    // create str fot db connect

    sDriver = "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

    // db conenct
    if sMachbaseConnect.ConnectDB(sDriver) == RC_FAILURE {
        fmt.Println(sMachbaseConnect.PrintConErr())
    } else {
        fmt.Println("Machbase Connect Success!!")
    }

    return
}

func CreateStatement() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sMachbaseStmt *machbase.MachbaseStmt = nil
    var sIp string = "127.0.0.1"
    var sPort string = "5656"
    var sId string = "SYS"
    var sPass string = "MANAGER"
    var sDriver string = ""

    // create connect struct
    sMachbaseConnect = machbase.CreateConnect()

    defer func() {
        if sMachbaseStmt.FreeStmt() == RC_FAILURE {
            fmt.Println("Machbase Free Statement Fail")
        } else {
            fmt.Println("Machbase Free Statement Success")
        }
        sMachbaseStmt = nil

        if sMachbaseConnect.DisconnectDB() == RC_FAILURE {
            fmt.Println("Machbase DisConnect Fail")
        } else {
            fmt.Println("Machbase DisConnect Success")
        }
        sMachbaseConnect = nil
    }()

    sDriver = "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

    // db conenct
    if sMachbaseConnect.ConnectDB(sDriver) == RC_FAILURE {
        fmt.Println(sMachbaseConnect.PrintConErr())
        return
    } else {
        fmt.Println("Machbase Connect Success!!")
    }

    sMachbaseStmt = sMachbaseConnect.CreateStmt()

    // stmt alloc
    if sMachbaseStmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("Machbase Statement Alloc Success!!")
    }

    return
}

func CreateTable() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sMachbaseStmt *machbase.MachbaseStmt = nil
    var sIp string = "127.0.0.1"
    var sPort string = "5656"
    var sId string = "SYS"
    var sPass string = "MANAGER"
    var sDriver string = ""
    var sSql string = ""

    // create connect struct
    sMachbaseConnect = machbase.CreateConnect()

    defer func() {
        if sMachbaseStmt.FreeStmt() == RC_FAILURE {
            fmt.Println("Machbase Free Statement Fail")
        } else {
            fmt.Println("Machbase Free Statement Success")
        }
        sMachbaseStmt = nil

        if sMachbaseConnect.DisconnectDB() == RC_FAILURE {
            fmt.Println("Machbase DisConnect Fail")
        } else {
            fmt.Println("Machbase DisConnect Success")
        }
        sMachbaseConnect = nil
    }()

    sDriver = "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

    // db conenct
    if sMachbaseConnect.ConnectDB(sDriver) == RC_FAILURE {
        fmt.Println(sMachbaseConnect.PrintConErr())
        return
    } else {
        fmt.Println("Machbase Connect Success!!")
    }

    sMachbaseStmt = sMachbaseConnect.CreateStmt()

    // stmt alloc
    if sMachbaseStmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(sMachbaseStmt.PrintStmtErr())
        return
    } else {
        fmt.Println("Machbase Statement Alloc Success!!")
    }

    sSql = "CREATE TABLE GO_SAMPLE(IDX INTEGER, D1 SHORT, D2 INTEGER, D3 LONG, F1 FLOAT, F2 DOUBLE, NAME VARCHAR(20), TEXT TEXT, IMAGE BINARY, V4 IPV4, V6 IPV6, DT DATETIME)"

    // create tag table
    if sMachbaseStmt.ExecDirect(sSql) == RC_SUCCESS {
        fmt.Println("Create Sample Table Success!!")
    } else {
        fmt.Println("Create Sample Table Fail : ",sMachbaseStmt.PrintStmtErr())
    }

    return
}

func DropTable() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sMachbaseStmt *machbase.MachbaseStmt = nil
    var sIp string = "127.0.0.1"
    var sPort string = "5656"
    var sId string = "SYS"
    var sPass string = "MANAGER"
    var sDriver string = ""
    var sSql string = ""

    // create connect struct
    sMachbaseConnect = machbase.CreateConnect()

    defer func() {
        if sMachbaseStmt.FreeStmt() == RC_FAILURE {
            fmt.Println("Machbase Free Statement Fail")
        } else {
            fmt.Println("Machbase Free Statement Success")
        }
        sMachbaseStmt = nil

        if sMachbaseConnect.DisconnectDB() == RC_FAILURE {
            fmt.Println("Machbase DisConnect Fail")
        } else {
            fmt.Println("Machbase DisConnect Success")
        }
        sMachbaseConnect = nil
    }()

    sDriver = "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

    // db conenct
    if sMachbaseConnect.ConnectDB(sDriver) == RC_FAILURE {
        fmt.Println(sMachbaseConnect.PrintConErr())
        return
    } else {
        fmt.Println("Machbase Connect Success!!")
    }

    sMachbaseStmt = sMachbaseConnect.CreateStmt()

    // stmt alloc
    if sMachbaseStmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(sMachbaseStmt.PrintStmtErr())
        return
    } else {
        fmt.Println("Machbase Statement Alloc Success!!")
    }

    sSql = "DROP TABLE GO_SAMPLE"

    // create tag table
    if sMachbaseStmt.ExecDirect(sSql) == RC_SUCCESS {
        fmt.Println("Drop Sample Table Success!!")
    } else {
        fmt.Println("Drop Sample Table Fail : ",sMachbaseStmt.PrintStmtErr())
    }

    return
}

func Append() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sMachbaseStmt *machbase.MachbaseStmt = nil
    var sIp string = "127.0.0.1"
    var sPort string = "5656"
    var sId string = "SYS"
    var sPass string = "MANAGER"
    var sDriver string = ""
    var sTable string = "GO_SAMPLE"
    var sLength int = 12
    var sType []int = make([]int, sLength)
    var sValue []string = make([]string, sLength)
    var sDateFormat string = ""
    //var sDateFormat string = "YYYY-MM-DD HH24:MI:SS mmm.uuu.nnn"  //use timeformat
    var sCount int = 0
    var sResult int = RC_FAILURE

    // create connect struct
    sMachbaseConnect = machbase.CreateConnect()

    defer func() {
        if sMachbaseStmt.FreeStmt() == RC_FAILURE {
            fmt.Println("Machbase Free Statement Fail")
        } else {
            fmt.Println("Machbase Free Statement Success")
        }
        sMachbaseStmt = nil

        if sMachbaseConnect.DisconnectDB() == RC_FAILURE {
            fmt.Println("Machbase DisConnect Fail")
        } else {
            fmt.Println("Machbase DisConnect Success")
        }
        sMachbaseConnect = nil
    }()

    sDriver = "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

    // db conenct
    if sMachbaseConnect.ConnectDB(sDriver) == RC_FAILURE {
        fmt.Println(sMachbaseConnect.PrintConErr())
        return
    } else {
        fmt.Println("Machbase Connect Success!!")
    }

    sMachbaseStmt = sMachbaseConnect.CreateStmt()

    // stmt alloc
    if sMachbaseStmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(sMachbaseStmt.PrintStmtErr())
        return
    } else {
        fmt.Println("Machbase Statement Alloc Success!!")
    }

    // data append open
    if sMachbaseStmt.AppendOpen(sTable) != RC_SUCCESS {
        fmt.Println("AppendOpen Fail : ",sMachbaseStmt.PrintStmtErr())
        return
    }

    for sCount < (sLength + 1) {
        sType[0] = machbase.MACHBASE_INTEGER
        sValue[0] = strconv.Itoa(sCount)

        sType[1] = machbase.MACHBASE_SHORT
        sValue[1] = strconv.Itoa(sCount)

        sType[2] = machbase.MACHBASE_INTEGER
        sValue[2] = strconv.Itoa(sCount + sCount)

        sType[3] = machbase.MACHBASE_LONG
        sValue[3] = strconv.Itoa((sCount + sCount) * 100)

        sType[4] = machbase.MACHBASE_FLOAT
        sValue[4] = strconv.Itoa(sCount)

        sType[5] = machbase.MACHBASE_DOUBLE
        sValue[5] = strconv.Itoa(sCount)

        sType[6] = machbase.MACHBASE_VARCHAR
        sValue[6] = "Varchar Test" + strconv.Itoa(sCount)

        sType[7] = machbase.MACHBASE_TEXT
        sValue[7] = "Text Test" + strconv.Itoa(sCount)

        sType[8] = machbase.MACHBASE_BINARY
        sValue[8] = base64.StdEncoding.EncodeToString([]byte("Binary Test" + strconv.Itoa(sCount)))

        sType[9] = machbase.MACHBASE_IPV4
        sValue[9] = "192.168.0." + strconv.Itoa(sCount)

        sType[10] = machbase.MACHBASE_IPV6
        sValue[10] = fmt.Sprintf("2001:0DB8:0000:0000:0000:0000:1428:%04d", sCount)

        sType[11] = machbase.MACHBASE_DATETIME
        sValue[11] = strconv.FormatInt(time.Now().UnixNano(), 10)
        //sValue[11] = time.Now().Format("2006-01-02 15:04:05")  //use timeformat

        for i := sCount; i < sLength; i++ {
            sValue[i] = ""
        }

        if sMachbaseStmt.AppendDataV2(sType, sValue, sDateFormat, sLength) != RC_SUCCESS {
            fmt.Println("AppendDataV2 Fail : ",sMachbaseStmt.PrintStmtErr())
        }

        sCount++
    }

    if sMachbaseStmt.AppendFlush() != RC_SUCCESS {
        fmt.Println("AppendFlush Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("AppendFlush Success!!")
    }

    sResult = sMachbaseStmt.AppendClose()

    if sResult == RC_FAILURE {
        fmt.Println("AppendClose Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("Success Count : ",sResult)
    }

    return
}

func Select() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sMachbaseStmt *machbase.MachbaseStmt = nil
    var sIp string = "127.0.0.1"
    var sPort string = "5656"
    var sId string = "SYS"
    var sPass string = "MANAGER"
    var sDriver string = ""
    var sSql string = ""
    var sInterfaceArr []interface{} = nil
    var sImage []byte = nil

    // create connect struct
    sMachbaseConnect = machbase.CreateConnect()

    defer func() {
        if sMachbaseStmt.FreeStmt() == RC_FAILURE {
            fmt.Println("Machbase Free Statement Fail")
        } else {
            fmt.Println("Machbase Free Statement Success")
        }
        sMachbaseStmt = nil

        if sMachbaseConnect.DisconnectDB() == RC_FAILURE {
            fmt.Println("Machbase DisConnect Fail")
        } else {
            fmt.Println("Machbase DisConnect Success")
        }
        sMachbaseConnect = nil
    }()

    sDriver = "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

    // db conenct
    if sMachbaseConnect.ConnectDB(sDriver) == RC_FAILURE {
        fmt.Println(sMachbaseConnect.PrintConErr())
        return
    } else {
        fmt.Println("Machbase Connect Success!!")
    }

    sMachbaseStmt = sMachbaseConnect.CreateStmt()

    // stmt alloc
    if sMachbaseStmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(sMachbaseStmt.PrintStmtErr())
        return
    } else {
        fmt.Println("Machbase Statement Alloc Success!!")
    }

    sSql = "SELECT * FROM GO_SAMPLE"
    if sMachbaseStmt.Prepare(sSql) != RC_SUCCESS {
        fmt.Println("Prepare Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("Prepare Success!!")
    }

    if sMachbaseStmt.Execute() != RC_SUCCESS {
        fmt.Println("Execute Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("Execute Success!!")
    }

    sInterfaceArr = make([]interface{}, sMachbaseStmt.GetColCount())
    for {
        if sMachbaseStmt.Fetch(sInterfaceArr) == RC_FAILURE {
            fmt.Println("Fetch Fail : ",sMachbaseStmt.PrintStmtErr())
            break
        }

        fmt.Println("IDX : ",sInterfaceArr[0])
        fmt.Println("D1 : ",sInterfaceArr[1])
        fmt.Println("D2 : ",sInterfaceArr[2])
        fmt.Println("D3 : ",sInterfaceArr[3])
        fmt.Println("F1 : ",sInterfaceArr[4])
        fmt.Println("F2 : ",sInterfaceArr[5])
        fmt.Println("NAME : ",sInterfaceArr[6])
        fmt.Println("TEXT : ",sInterfaceArr[7])
        if sInterfaceArr[8] != nil {
            sImage, _ = base64.StdEncoding.DecodeString(sInterfaceArr[8].(string))
            fmt.Println("IMAGE : ",sImage)
        } else {
            fmt.Println("IMAGE : ",sInterfaceArr[8])
        }
        fmt.Println("IPV4 : ",sInterfaceArr[9])
        fmt.Println("IPV6 : ",sInterfaceArr[10])
        fmt.Println("TIME : ",sInterfaceArr[11])
        fmt.Println("*****************************************")
    }

    return
}

func ImageAppend() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sMachbaseStmt *machbase.MachbaseStmt = nil
    var sIp string = "127.0.0.1"
    var sPort string = "5656"
    var sId string = "SYS"
    var sPass string = "MANAGER"
    var sDriver string = ""
    var sTable string = "GO_SAMPLE"
    var sLength int = 12
    var sType []int = make([]int, sLength)
    var sValue []string = make([]string, sLength)
    var sDateFormat string = ""
    var sResult int = RC_FAILURE

    // create connect struct
    sMachbaseConnect = machbase.CreateConnect()

    defer func() {
        if sMachbaseStmt.FreeStmt() == RC_FAILURE {
            fmt.Println("Machbase Free Statement Fail")
        } else {
            fmt.Println("Machbase Free Statement Success")
        }
        sMachbaseStmt = nil

        if sMachbaseConnect.DisconnectDB() == RC_FAILURE {
            fmt.Println("Machbase DisConnect Fail")
        } else {
            fmt.Println("Machbase DisConnect Success")
        }
        sMachbaseConnect = nil
    }()

    sDriver = "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

    // db conenct
    if sMachbaseConnect.ConnectDB(sDriver) == RC_FAILURE {
        fmt.Println(sMachbaseConnect.PrintConErr())
        return
    } else {
        fmt.Println("Machbase Connect Success!!")
    }

    sMachbaseStmt = sMachbaseConnect.CreateStmt()

    // stmt alloc
    if sMachbaseStmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(sMachbaseStmt.PrintStmtErr())
        return
    } else {
        fmt.Println("Machbase Statement Alloc Success!!")
    }

    // data append open
    if sMachbaseStmt.AppendOpen(sTable) != RC_SUCCESS {
        fmt.Println("AppendOpen Fail : ",sMachbaseStmt.PrintStmtErr())
        return
    }

    sType[0] = machbase.MACHBASE_INTEGER
    sValue[0] = ""

    sType[1] = machbase.MACHBASE_SHORT
    sValue[1] = ""

    sType[2] = machbase.MACHBASE_INTEGER
    sValue[2] = ""

    sType[3] = machbase.MACHBASE_LONG
    sValue[3] = ""

    sType[4] = machbase.MACHBASE_FLOAT
    sValue[4] = ""

    sType[5] = machbase.MACHBASE_DOUBLE
    sValue[5] = ""

    sType[6] = machbase.MACHBASE_VARCHAR
    sValue[6] = ""

    sType[7] = machbase.MACHBASE_TEXT
    sValue[7] = ""

    sType[8] = machbase.MACHBASE_BINARY
    sValue[8] = readFile()

    sType[9] = machbase.MACHBASE_IPV4
    sValue[9] = ""

    sType[10] = machbase.MACHBASE_IPV6
    sValue[10] = ""

    sType[11] = machbase.MACHBASE_DATETIME
    sValue[11] = ""

    if sMachbaseStmt.AppendDataV2(sType, sValue, sDateFormat, sLength) != RC_SUCCESS {
        fmt.Println("AppendDataV2 Fail : ",sMachbaseStmt.PrintStmtErr())
    }

    if sMachbaseStmt.AppendFlush() != RC_SUCCESS {
        fmt.Println("AppendFlush Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("AppendFlush Success!!")
    }

    sResult = sMachbaseStmt.AppendClose()

    if sResult == RC_FAILURE {
        fmt.Println("AppendClose Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("Success Count : ",sResult)
    }

    return
}

func readFile() string {
    sPath, err := os.Getwd()
    if err != nil {
        fmt.Println("Folder Path Error : ",err)
    }

    sPath = sPath + "/Machbase.png"

    sByte, err := ioutil.ReadFile(sPath)
    if err != nil {
        fmt.Println("readFile Error : ",err)
    }

    return base64.StdEncoding.EncodeToString(sByte)
}

func ImageSelect() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sMachbaseStmt *machbase.MachbaseStmt = nil
    var sIp string = "127.0.0.1"
    var sPort string = "5656"
    var sId string = "SYS"
    var sPass string = "MANAGER"
    var sDriver string = ""
    var sSql string = ""
    var sInterfaceArr []interface{} = nil

    // create connect struct
    sMachbaseConnect = machbase.CreateConnect()

    defer func() {
        if sMachbaseStmt.FreeStmt() == RC_FAILURE {
            fmt.Println("Machbase Free Statement Fail")
        } else {
            fmt.Println("Machbase Free Statement Success")
        }
        sMachbaseStmt = nil

        if sMachbaseConnect.DisconnectDB() == RC_FAILURE {
            fmt.Println("Machbase DisConnect Fail")
        } else {
            fmt.Println("Machbase DisConnect Success")
        }
        sMachbaseConnect = nil
    }()

    sDriver = "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

    // db conenct
    if sMachbaseConnect.ConnectDB(sDriver) == RC_FAILURE {
        fmt.Println(sMachbaseConnect.PrintConErr())
        return
    } else {
        fmt.Println("Machbase Connect Success!!")
    }

    sMachbaseStmt = sMachbaseConnect.CreateStmt()

    // stmt alloc
    if sMachbaseStmt.AllocStmt() != RC_SUCCESS {
        fmt.Println(sMachbaseStmt.PrintStmtErr())
        return
    } else {
        fmt.Println("Machbase Statement Alloc Success!!")
    }

    sSql = "SELECT IMAGE FROM GO_SAMPLE"
    if sMachbaseStmt.Prepare(sSql) != RC_SUCCESS {
        fmt.Println("Prepare Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("Prepare Success!!")
    }

    if sMachbaseStmt.Execute() != RC_SUCCESS {
        fmt.Println("Execute Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("Execute Success!!")
    }

    sInterfaceArr = make([]interface{}, sMachbaseStmt.GetColCount())

    if sMachbaseStmt.Fetch(sInterfaceArr) == RC_FAILURE {
        fmt.Println("Fetch Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        saveFile(sInterfaceArr[0].(string))
    }

    return
}

func saveFile(aData string) {
    sPath, err := os.Getwd()
    if err != nil {
        fmt.Println("Folder Path Error : ",err)
    }

    sPath = sPath + "/Machbase2.png"

    sFile, err := os.OpenFile(sPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0664))
    if err != nil {
        fmt.Println("error:", err)
    }

    sData, _ := base64.StdEncoding.DecodeString(aData)
    _, err = sFile.Write(sData)
    if err != nil {
        fmt.Println("error:", err)
    }
}