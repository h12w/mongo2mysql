package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"gopkg.in/pipe.v2"
)

type (
	Config struct {
		Config string  `long:"config"`
		Mongo  Mongo   `yaml:"mongo"`
		MySQL  MySQL   `yaml:"mysql" group:"mysql" namespace:"mysql"`
		Tables []Table `yaml:"tables"`
	}
	Mongo struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
		DB   string `yaml:"db"`
	}
	MySQL struct {
		LoginPath string `yaml:"login_path"`
		DB        string `yaml:"db" long:"db"`
	}
	Table struct {
		MongoName string `yaml:"mongo_name"`
		MySQLName string `yaml:"mysql_name"`
		Fields    Fields `yaml:"fields"`
		CreateCmd string `yaml:"create_cmd"`
		AfterCmd  string `yaml:"after_cmd"`
	}

	Record     map[string]interface{}
	Fields     []string
	FieldPath  []string
	FieldPaths []FieldPath
)

func (table *Table) Process(cfg *Config) error {
	p := pipe.Line(
		pipe.Exec("mongoexport",
			"--host", cfg.Mongo.Host,
			"--port", strconv.Itoa(cfg.Mongo.Port),
			"--db", cfg.Mongo.DB,
			"--collection", table.MongoName,
			"--type=json"),
		pipe.Replace(table.Fields.expand().convertLine),
		pipe.Exec("mysql",
			"--login-path="+cfg.MySQL.LoginPath,
			"--compress=TRUE",
			"--database", cfg.MySQL.DB,
			"--verbose",
			"--execute", table.CreateCmd+
				";LOAD DATA LOCAL INFILE '/dev/stdin' IGNORE INTO TABLE "+table.MySQLName+","+
				table.AfterCmd),
		pipe.Write(os.Stdout),
	)
	return pipe.Run(p)
}

func (rec Record) Get(path FieldPath) string {
	if len(path) > 1 {
		subrec, _ := rec[path[0]].(map[string]interface{})
		return Record(subrec).Get(path[1:])
	} else if obj, ok := rec[path[0]]; ok {
		switch v := obj.(type) {
		case nil:
			return ""
		case int:
			return strconv.Itoa(v)
		case float64:
			if math.Mod(v, 1.0) == 0.0 {
				return strconv.Itoa(int(v))
			} else {
				return fmt.Sprintf("%f", v)
			}
		case []interface{}:
			ss := make([]string, len(v))
			for i, value := range v {
				switch value.(type) {
				case string:
					ss[i] = value.(string)
				default:
					return fmt.Sprintf("%+v", v)
				}
			}
			return strings.Join(ss, "|")
		case string:
			return v
		default:
			return fmt.Sprintf("%+v", v)
		}
	}

	return ""
}

func clean(s string) string {
	s = strings.Replace(s, "\t", `\t`, -1)
	s = strings.Replace(s, "\n", `\n`, -1)
	return s
}

func (rec Record) ToCSV(fields FieldPaths) []byte {
	var record []string
	for _, path := range fields {
		record = append(record, clean(rec.Get(path)))
	}
	var w bytes.Buffer
	csvWriter := csv.NewWriter(&w)
	csvWriter.Comma = '\t'
	if err := csvWriter.Write(record); err != nil {
		panic(err)
	}
	csvWriter.Flush()
	return w.Bytes()
}

func (fields Fields) expand() FieldPaths {
	var paths []FieldPath
	for _, key := range fields {
		paths = append(paths, strings.Split(key, "."))
	}
	return paths
}

func (fields FieldPaths) convertLine(line []byte) []byte {
	var rec Record
	if err := json.Unmarshal(line, &rec); err != nil {
		panic(err)
	}
	return rec.ToCSV(fields)
}
