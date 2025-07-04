// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package open

import (
	"bytes"
	"encoding/binary"
	"strconv"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func encodeRowChangedEvent(
	e *commonEvent.RowEvent,
	columnFlags map[string]uint64,
	config *common.Config,
	largeMessageOnlyHandleKeyColumns bool,
	claimCheckLocationName string,
) ([]byte, []byte, int, error) {
	var (
		keyBuf   bytes.Buffer
		valueBuf bytes.Buffer
	)
	keyWriter := util.BorrowJSONWriter(&keyBuf)
	valueWriter := util.BorrowJSONWriter(&valueBuf)

	keyWriter.WriteObject(func() {
		keyWriter.WriteUint64Field("ts", e.CommitTs)
		keyWriter.WriteStringField("scm", e.TableInfo.GetSchemaName())
		keyWriter.WriteStringField("tbl", e.TableInfo.GetTableName())
		keyWriter.WriteIntField("t", int(common.MessageTypeRow))

		if largeMessageOnlyHandleKeyColumns {
			keyWriter.WriteBoolField("ohk", true)
		}
		if claimCheckLocationName != "" {
			keyWriter.WriteBoolField("ohk", false)
			keyWriter.WriteStringField("ccl", claimCheckLocationName)
		}
		if e.TableInfo.IsPartitionTable() {
			keyWriter.WriteInt64Field("ptn", e.GetTableID())
		}
	})
	var err error
	if e.IsDelete() {
		onlyHandleKeyColumns := config.DeleteOnlyHandleKeyColumns || largeMessageOnlyHandleKeyColumns
		valueWriter.WriteObject(func() {
			valueWriter.WriteObjectField("d", func() {
				err = writeColumnFieldValues(valueWriter, e.GetPreRows(), e.TableInfo, columnFlags, e.ColumnSelector, onlyHandleKeyColumns)
			})
		})
	} else if e.IsInsert() {
		valueWriter.WriteObject(func() {
			valueWriter.WriteObjectField("u", func() {
				err = writeColumnFieldValues(valueWriter, e.GetRows(), e.TableInfo, columnFlags, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
			})
		})
	} else if e.IsUpdate() {
		valueWriter.WriteObject(func() {
			valueWriter.WriteObjectField("u", func() {
				err = writeColumnFieldValues(valueWriter, e.GetRows(), e.TableInfo, columnFlags, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
			})
			if err != nil {
				return
			}
			if !config.OnlyOutputUpdatedColumns {
				valueWriter.WriteObjectField("p", func() {
					err = writeColumnFieldValues(valueWriter, e.GetPreRows(), e.TableInfo, columnFlags, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
				})
			} else {
				valueWriter.WriteObjectField("p", func() {
					writeUpdatedColumnFieldValues(valueWriter, e.GetPreRows(), e.GetRows(), e.TableInfo, columnFlags, e.ColumnSelector, largeMessageOnlyHandleKeyColumns)
				})
			}
		})
	}
	util.ReturnJSONWriter(keyWriter)
	util.ReturnJSONWriter(valueWriter)

	if err != nil {
		return nil, nil, 0, err
	}

	key := keyBuf.Bytes()
	value := valueBuf.Bytes()

	valueCompressed, err := common.Compress(
		config.ChangefeedID, config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, nil, 0, err
	}

	// for single message that is longer than max-message-bytes
	// 16 is the length of `keyLenByte` and `valueLenByte`, 8 is the length of `versionHead`
	length := len(key) + len(valueCompressed) + common.MaxRecordOverhead + 16 + 8
	return key, valueCompressed, length, nil
}

func encodeDDLEvent(e *commonEvent.DDLEvent, config *common.Config) ([]byte, []byte, error) {
	keyBuf := &bytes.Buffer{}
	valueBuf := &bytes.Buffer{}
	keyWriter := util.BorrowJSONWriter(keyBuf)
	valueWriter := util.BorrowJSONWriter(valueBuf)

	keyWriter.WriteObject(func() {
		keyWriter.WriteUint64Field("ts", e.FinishedTs)
		keyWriter.WriteStringField("scm", e.SchemaName)
		keyWriter.WriteStringField("tbl", e.TableName)
		keyWriter.WriteIntField("t", int(common.MessageTypeDDL))
	})

	valueWriter.WriteObject(func() {
		valueWriter.WriteStringField("q", e.Query)
		valueWriter.WriteIntField("t", int(e.Type))
	})

	util.ReturnJSONWriter(keyWriter)
	util.ReturnJSONWriter(valueWriter)

	value, err := common.Compress(
		config.ChangefeedID, config.LargeMessageHandle.LargeMessageHandleCompression, valueBuf.Bytes(),
	)
	if err != nil {
		return nil, nil, err
	}

	key := keyBuf.Bytes()

	var keyLenByte [8]byte
	var valueLenByte [8]byte
	var versionByte [8]byte

	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))
	binary.BigEndian.PutUint64(versionByte[:], batchVersion1)

	keyOutput := new(bytes.Buffer)
	keyOutput.Write(versionByte[:])
	keyOutput.Write(keyLenByte[:])
	keyOutput.Write(key)

	valueOutput := new(bytes.Buffer)
	valueOutput.Write(valueLenByte[:])
	valueOutput.Write(value)

	return keyOutput.Bytes(), valueOutput.Bytes(), nil
}

func writeColumnFieldValue(
	writer *util.JSONWriter,
	col *model.ColumnInfo,
	row *chunk.Row,
	idx int,
	isHandle bool,
	columnFlag uint64,
) {
	fieldType := col.FieldType
	writer.WriteIntField("t", int(fieldType.GetType()))
	if isHandle {
		writer.WriteBoolField("h", isHandle)
	}
	writer.WriteUint64Field("f", columnFlag)

	if row.IsNull(idx) {
		writer.WriteNullField("v")
		return
	}

	switch fieldType.GetType() {
	case mysql.TypeBit:
		d := row.GetDatum(idx, &fieldType)
		dp := &d
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		value, _ := dp.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		writer.WriteUint64Field("v", value)
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		value := row.GetBytes(idx)
		writer.WriteBase64StringField("v", value)
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		value := row.GetBytes(idx)
		if mysql.HasBinaryFlag(fieldType.GetFlag()) {
			str := string(value)
			str = strconv.Quote(str)
			str = str[1 : len(str)-1]
			writer.WriteStringField("v", str)
		} else {
			writer.WriteStringField("v", string(value))
		}
	case mysql.TypeEnum, mysql.TypeSet:
		value := row.GetEnum(idx).Value
		writer.WriteUint64Field("v", value)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		value := row.GetTime(idx)
		writer.WriteStringField("v", value.String())
	case mysql.TypeDuration:
		value := row.GetDuration(idx, 0)
		writer.WriteStringField("v", value.String())
	case mysql.TypeJSON:
		value := row.GetJSON(idx)
		writer.WriteStringField("v", value.String())
	case mysql.TypeNewDecimal:
		value := row.GetMyDecimal(idx)
		writer.WriteStringField("v", value.String())
	case mysql.TypeTiDBVectorFloat32:
		value := row.GetVectorFloat32(idx).String()
		writer.WriteStringField("v", value)
	default:
		d := row.GetDatum(idx, &fieldType)
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		value := d.GetValue()
		writer.WriteAnyField("v", value)
	}
}

func writeColumnFieldValues(
	jWriter *util.JSONWriter,
	row *chunk.Row,
	tableInfo *commonType.TableInfo,
	columnFlags map[string]uint64,
	selector columnselector.Selector,
	onlyHandleKeyColumns bool,
) error {
	var encoded bool
	colInfo := tableInfo.GetColumns()
	for idx, col := range colInfo {
		if selector.Select(col) {
			if col.IsVirtualGenerated() {
				continue
			}
			handle := tableInfo.IsHandleKey(col.ID)
			if onlyHandleKeyColumns && !handle {
				continue
			}
			encoded = true
			jWriter.WriteObjectField(col.Name.O, func() {
				writeColumnFieldValue(jWriter, col, row, idx, handle, columnFlags[col.Name.O])
			})
		}
	}
	if !encoded {
		return errors.ErrOpenProtocolCodecInvalidData.GenWithStack("not found handle key columns for the delete event")
	}
	return nil
}

func writeUpdatedColumnFieldValues(
	jWriter *util.JSONWriter,
	preRow *chunk.Row,
	row *chunk.Row,
	tableInfo *commonType.TableInfo,
	columnFlags map[string]uint64,
	selector columnselector.Selector,
	onlyHandleKeyColumns bool,
) {
	// we don't need check here whether after column selector there still exists handle key column
	// because writeUpdatedColumnFieldValues only can be called after successfully dealing with one row event
	colInfo := tableInfo.GetColumns()

	for idx, col := range colInfo {
		if selector.Select(col) {
			isHandle := tableInfo.IsHandleKey(col.ID)
			if onlyHandleKeyColumns && !isHandle {
				continue
			}
			writeColumnFieldValueIfUpdated(jWriter, col, preRow, row, idx, isHandle, columnFlags[col.Name.O])
		}
	}
}

func writeColumnFieldValueIfUpdated(
	writer *util.JSONWriter,
	col *model.ColumnInfo,
	preRow *chunk.Row,
	row *chunk.Row,
	idx int,
	isHandle bool,
	columnFlag uint64,
) {
	colType := col.GetType()
	flag := col.GetFlag()

	writeFunc := func(writeColumnValue func()) {
		writer.WriteObjectField(col.Name.O, func() {
			writer.WriteIntField("t", int(colType))
			if isHandle {
				writer.WriteBoolField("h", isHandle)
			}
			writer.WriteUint64Field("f", uint64(flag))
			writeColumnValue()
		})
	}

	if row.IsNull(idx) && preRow.IsNull(idx) {
		return
	}
	if preRow.IsNull(idx) && !row.IsNull(idx) {
		writeFunc(func() { writer.WriteNullField("v") })
		return
	}
	if !preRow.IsNull(idx) && row.IsNull(idx) {
		writeColumnFieldValue(writer, col, preRow, idx, isHandle, columnFlag)
		return
	}

	switch col.GetType() {
	case mysql.TypeBit:
		rowDatum := row.GetDatum(idx, &col.FieldType)
		rowDatumPoint := &rowDatum
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		rowValue, _ := rowDatumPoint.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)

		preRowDatum := row.GetDatum(idx, &col.FieldType)
		preRowDatumPoint := &preRowDatum
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		preRowValue, _ := preRowDatumPoint.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)

		if rowValue != preRowValue {
			writeFunc(func() { writer.WriteUint64Field("v", preRowValue) })
		}
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		rowValue := row.GetBytes(idx)
		preRowValue := preRow.GetBytes(idx)
		if !bytes.Equal(rowValue, preRowValue) {
			if len(preRowValue) == 0 {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteBase64StringField("v", preRowValue) })
			}
		}
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		rowValue := row.GetBytes(idx)
		preRowValue := preRow.GetBytes(idx)
		if !bytes.Equal(rowValue, preRowValue) {
			if len(preRowValue) == 0 {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				if mysql.HasBinaryFlag(flag) {
					str := string(preRowValue)
					str = strconv.Quote(str)
					str = str[1 : len(str)-1]
					writeFunc(func() { writer.WriteStringField("v", str) })
				} else {
					writeFunc(func() { writer.WriteStringField("v", string(preRowValue)) })
				}
			}
		}
	case mysql.TypeEnum, mysql.TypeSet:
		rowValue := row.GetEnum(idx).Value
		preRowValue := preRow.GetEnum(idx).Value
		if rowValue != preRowValue {
			if preRowValue == 0 {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteUint64Field("v", preRowValue) })
			}
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		rowValue := row.GetTime(idx)
		preRowValue := preRow.GetTime(idx)
		if rowValue != preRowValue {
			if preRowValue.IsZero() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preRowValue.String()) })
			}
		}
	case mysql.TypeDuration:
		rowValue := row.GetDuration(idx, 0)
		preRowValue := preRow.GetDuration(idx, 0)
		if rowValue != preRowValue {
			if preRowValue.ToNumber().IsZero() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preRowValue.String()) })
			}
		}
	case mysql.TypeJSON:
		rowValue := row.GetJSON(idx).String()
		preRowValue := preRow.GetJSON(idx).String()
		if rowValue != preRowValue {
			if preRow.GetJSON(idx).IsZero() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preRowValue) })
			}
		}
	case mysql.TypeNewDecimal:
		rowValue := row.GetMyDecimal(idx)
		preValue := preRow.GetMyDecimal(idx)
		if rowValue.Compare(preValue) != 0 {
			if preValue.IsZero() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preValue.String()) })
			}
		}
	case mysql.TypeTiDBVectorFloat32:
		rowValue := row.GetVectorFloat32(idx)
		preValue := preRow.GetVectorFloat32(idx)
		if rowValue.Compare(preValue) != 0 {
			if preValue.IsZeroValue() {
				writeFunc(func() { writer.WriteNullField("v") })
			} else {
				writeFunc(func() { writer.WriteStringField("v", preValue.String()) })
			}
		}
	default:
		rowDatum := row.GetDatum(idx, &col.FieldType)
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		rowValue := rowDatum.GetValue()

		preRowDatum := preRow.GetDatum(idx, &col.FieldType)
		preRowValue := preRowDatum.GetValue()

		if rowValue != preRowValue {
			writeFunc(func() { writer.WriteAnyField("v", preRowValue) })
		}
	}
	return
}
