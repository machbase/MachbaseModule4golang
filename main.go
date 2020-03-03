package main

import (
	"fmt"
    "machbase"
    "strconv"
    "time"
    "math/rand"
)

var RC_SUCCESS int = 0
var RC_FAILURE int = -1

func main() {
    ConnectMachbase()
    CreateStatement()
    CreateTable()
    Append()
    Select()
}

func ConnectMachbase() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil

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
    sIp := "127.0.0.1"
    sPort := "5656"
    sId := "SYS"
    sPass := "MANAGER"
    sDriver := "SERVER=" + sIp + ";UID=" + sId + ";PWD=" + sPass + ";CONNTYPE=1;PORT_NO=" + sPort + ";CONNECTION_TIMEOUT=3"

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
    
    sDriver := "SERVER=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=5656;CONNECTION_TIMEOUT=3"
    
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
    
    sDriver := "SERVER=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=5656;CONNECTION_TIMEOUT=3"
    
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

    sSql := "create tagdata table TAG (name varchar(80) primary key, time datetime basetime, value double summarized)"

    // create tag table
    if sMachbaseStmt.ExecDirect(sSql) == RC_SUCCESS {
        fmt.Println("Create Tag Table Success!!")
    } else {
        fmt.Println("Create Tag Table Fail : ",sMachbaseStmt.PrintStmtErr())
    }
    
    return
}

func Append() {
    var sMachbaseConnect *machbase.MachbaseConnect = nil
    var sMachbaseStmt *machbase.MachbaseStmt = nil
    
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
    
    sDriver := "SERVER=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=5656;CONNECTION_TIMEOUT=3"
    
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

    sTable := "TAG"

    // data append open
    if sMachbaseStmt.AppendOpen(sTable) != RC_SUCCESS {
        fmt.Println("AppendOpen Fail : ",sMachbaseStmt.PrintStmtErr())
        return
    }

    sType := make([]int, 3)
    sValue := make([]string, 3)
    //var sDateFormat string = "YYYY-MM-DD HH24:MI:SS mmm.uuu.nnn"  //use timeformat
    var sDateFormat string = ""
    var sLen int = 3
    sCount := 0

    for sCount < 100 {
        sType[0] = machbase.MACHBASE_VARCHAR
        sValue[0] = "X1"
        
        sType[1] = machbase.MACHBASE_DATETIME
        //sValue[1] = time.Now().Format("2006-01-02 15:04:05")  //use timeformat
        sValue[1] = strconv.FormatInt(time.Now().UnixNano(), 10)
        
        sType[2] = machbase.MACHBASE_DOUBLE
        s1 := rand.NewSource(time.Now().UnixNano())
        r1 := rand.New(s1)
        sValue[2] = strconv.Itoa(r1.Intn(100))
        
        if sMachbaseStmt.AppendDataV2(sType, sValue, sDateFormat, sLen) != RC_SUCCESS {
            fmt.Println("AppendDataV2 Fail : ",sMachbaseStmt.PrintStmtErr())
        }

        sCount++
    }

    if sMachbaseStmt.AppendFlush() != RC_SUCCESS {
        fmt.Println("AppendFlush Fail : ",sMachbaseStmt.PrintStmtErr())
    } else {
        fmt.Println("AppendFlush Success!!")
    }

    sResult := sMachbaseStmt.AppendClose()

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
    
    sDriver := "SERVER=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=5656;CONNECTION_TIMEOUT=3"
    
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

    sSql := "select * from tag;"
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

    sInterfaceArr := make([]interface{}, sMachbaseStmt.GetColCount())
    for {
        if sMachbaseStmt.Fetch(sInterfaceArr) == RC_FAILURE {
            fmt.Println("Fetch Fail : ",sMachbaseStmt.PrintStmtErr())
            break
        }

        fmt.Println("name : ",sInterfaceArr[0])
        fmt.Println("time : ",sInterfaceArr[1])
        fmt.Println("value : ",sInterfaceArr[2])
        fmt.Println("*****************************************")
    }

    return
}