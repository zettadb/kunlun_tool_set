package restoreUtil

import (
	"reflect"
	"testing"
	"zetta_util/util/configParse"
)

func TestDoRestoreColdbackType_preCheckInstance(t *testing.T) {
	config := &configParse.RestoreUtilArguments{
		ColdBackupFilePath:     "/home/snow/play_ground/backup/base.tgz",
		BinlogBackupFilePath:   "/home/snow",
		MysqlParam:             nil,
		GlobalConsistentEnable: false,
		RestoreTime:            "",
	}
	optFile := &configParse.MysqlOptionFile{
		Path:       "/home/snow/play_ground/mysqlinstall/debug_install/etc/8002.cnf",
		Parameters: make(map[string]string, 0),
		Inited:     false,
	}
	config.MysqlParam = optFile
	type fields struct {
		Param *configParse.RestoreUtilArguments
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
		{name: "test1", fields: fields{config}, want: true, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dx := &DoRestoreColdbackType{
				Param: tt.fields.Param,
			}
			err := dx.preCheckInstance()
			if (err != nil) != tt.wantErr {
				t.Errorf("preCheckInstance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestDoRestoreColdbackType_extractColdBackup(t *testing.T) {
	config := &configParse.RestoreUtilArguments{
		ColdBackupFilePath:     "/home/snow/play_ground/backup/base.tgz",
		BinlogBackupFilePath:   "/home/snow",
		MysqlParam:             nil,
		GlobalConsistentEnable: false,
		RestoreTime:            "",
	}
	optFile := &configParse.MysqlOptionFile{
		Path:       "/home/snow/play_ground/mysqlinstall/debug_install/etc/8002.cnf",
		Parameters: make(map[string]string, 0),
		Inited:     false,
	}
	config.MysqlParam = optFile
	type fields struct {
		Param *configParse.RestoreUtilArguments
	}
	tests := []struct {
		name   string
		fields fields
		want   error
		want1  string
	}{
		// TODO: Add test cases.
		{name: "test1", fields: fields{config}, want: nil, want1: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dx := &DoRestoreColdbackType{
				Param: tt.fields.Param,
			}
			got, got1 := dx.extractColdBackup()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractColdBackup() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("extractColdBackup() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
