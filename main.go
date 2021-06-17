package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/gosuri/uiprogress"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	sharding    int
	tableName   string
	csvFileName string
	dialect     string
	dsn         string
	withHead    bool
)

var mtx sync.RWMutex

func init() {
	flag.IntVar(&sharding, "sharding", 1, "sharding count that may speed up export progress")
	flag.StringVar(&tableName, "tableName", "", "table name")
	flag.StringVar(&csvFileName, "csvFileName", "", "exported csv file name")
	flag.StringVar(&dialect, "dialect", "", "db dialect(mysql, sqlserver, postgres, sqlite are supported for now)")
	flag.StringVar(&dsn, "dsn", "", "db dsn")
	flag.BoolVar(&withHead, "withHead", true, "with csv head")
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	var totalCount int
	db := getDB()
	db.Raw(fmt.Sprintf("select count(*) from %s", tableName)).Scan(&totalCount)

	checkCmdParams(totalCount)

	f, err := os.OpenFile(csvFileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		f.Close()
		sqlDB, _ := db.DB()
		sqlDB.Close()
	}()

	if isOldVersionSqlserver(db) {
		sharding = 1
	}

	var wg = new(sync.WaitGroup)
	wg.Add(sharding)

	uiprogress.Start()

	//100 =>33,33,34 =>0-32,33-65,66-99
	shardingSize := totalCount / sharding
	for i := 0; i < sharding; i++ {
		min := i * shardingSize
		max := min + shardingSize - 1
		if i == sharding-1 {
			max = totalCount - 1
		}

		go func(i int) {
			t := NewExportThread(f, db, min, max, wg, fmt.Sprintf("thread-%d", i))
			t.WriteCSV()
		}(i)
	}

	wg.Wait()
	uiprogress.Stop()
}

type ExportThread struct {
	w          *csv.Writer
	db         *gorm.DB
	min        int
	max        int
	wg         *sync.WaitGroup
	threadName string
}

func NewExportThread(f *os.File, db *gorm.DB, min int, max int, wg *sync.WaitGroup, threadName string) *ExportThread {
	return &ExportThread{csv.NewWriter(f), db, min, max, wg, threadName}
}

func (t *ExportThread) WriteCSV() error {
	defer t.wg.Done()

	db := t.db.Table(tableName)
	if !isOldVersionSqlserver(db) {
		db = db.Offset(t.min).Limit(t.max - t.min + 1)
	}

	rows, err := db.Rows()
	if err != nil {
		return err
	}

	mtx.Lock()
	if withHead {
		columns, err := rows.Columns()
		if err != nil {
			return err
		}
		if err := t.w.Write(columns); err != nil {
			return err
		}
		t.w.Flush()
		withHead = false
	}
	mtx.Unlock()

	defer func() {
		rows.Close()
	}()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	//values := make([]interface{}, len(columns))
	//
	//for i := range values {
	//	var a string
	//	values[i] = &a
	//}
	//
	//i := 1
	//
	//for rows.Next() {
	//	record := make([]string, len(columns))
	//	rows.Scan(values...)
	//
	//	for i, value := range values {
	//		record[i] = *value.(*string)
	//	}
	//
	//	t.w.Write(record)
	//
	//	if i%1000 == 0 {
	//		t.w.Flush()
	//	}
	//	i++
	//}

	rawResult := make([][]byte, len(columns))
	result := make([]string, len(columns))

	dest := make([]interface{}, len(columns))
	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	total := t.max - t.min + 1
	bar := uiprogress.AddBar(total).AppendCompleted().PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("[%s] (%d/%d)", t.threadName, b.Current(), total)
	})

	for i := 1; rows.Next(); i++ {
		rows.Scan(dest...)
		for i, raw := range rawResult {
			if raw == nil {
				result[i] = ""
			} else {
				result[i] = string(raw)
			}
		}
		t.w.Write(result)
		bar.Incr()

		if i%1000 == 0 {
			t.w.Flush()
			if err := t.w.Error(); err != nil {
				log.Fatal(err)
			}
		}
	}

	defer func() {
		t.w.Flush()
	}()

	return nil
}

func getDialector(dialect string, dsn string) gorm.Dialector {
	var dialector gorm.Dialector

	switch dialect {
	case "mysql":
		dialector = mysql.Open(dsn)
	case "postgres":
		dialector = postgres.Open(dsn)
	case "sqlite":
		dialector = sqlite.Open(dsn)
	case "sqlserver":
		dialector = sqlserver.Open(dsn)
	default:
		log.Fatal("Unsupported dialect")
	}

	return dialector
}

func getDB() *gorm.DB {
	dialector := getDialector(dialect, dsn)
	db, err := gorm.Open(dialector, &gorm.Config{
		PrepareStmt: true,
		Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             10 * time.Second, // Slow SQL threshold
				LogLevel:                  logger.Silent,    // Log level
				IgnoreRecordNotFoundError: true,             // Ignore ErrRecordNotFound error for logger
				Colorful:                  false,            // Disable color
			},
		),
	})
	if err != nil {
		log.Fatal(err)
	}
	//db = db.Debug()

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}

	sqlDB.SetMaxIdleConns(sharding)
	sqlDB.SetMaxOpenConns(sharding)
	sqlDB.SetConnMaxLifetime(-1)

	return db
}

func checkCmdParams(totalCount int) {
	if totalCount == 0 {
		log.Fatal("no data to export")
	}
	if sharding <= 0 {
		sharding = 1
	}
	if sharding > totalCount {
		sharding = totalCount
	}
	if isBlank(tableName) {
		log.Fatal("tableName is required")
	}
	if isBlank(dialect) {
		log.Fatal("dialect is required")
	}
	if isBlank(dsn) {
		log.Fatal("dsn is required")
	}
	if isBlank(csvFileName) {
		csvFileName = tableName + ".csv"
	}
}

func isBlank(s string) bool {
	return strings.TrimSpace(s) == ""
}

func isOldVersionSqlserver(db *gorm.DB) bool {
	if db.Dialector.Name() == "sqlserver" {
		var v string
		sqlDB, _ := db.DB()
		sqlDB.QueryRow("select @@version").Scan(&v)
		re := regexp.MustCompile(`(\d+).\d+.\d+\.\d+`)
		args := re.FindStringSubmatch(v)
		majorVer, _ := strconv.Atoi(args[1])
		return majorVer <= 10
	}
	return false
}
