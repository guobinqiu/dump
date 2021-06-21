Fast CSV Exporter

Faster than navicat. Can used to export `mysql, sqlserver, postgres, sqlite` to csv file.

CLI Params:

Name|Required|Value
---|---|---
sharding|no|default: `1`
tableName|yes
csvFileName|no|default: `tableName + ".csv"`
dialect|yes|`mysql, sqlserver, postgres, sqlite` are supported for now
dsn|yes|https://gorm.io/docs/connecting_to_the_database.html
withHead|no|true or false. Default to `true`

Example:

`go run main.go -dialect mysql -tableName <tableName> -dsn "<username>:<password>@tcp(<host>:<port>)/<databaseName>?charset=utf8mb4&parseTime=True&loc=Local"`

![Demo](img/demo.png)
