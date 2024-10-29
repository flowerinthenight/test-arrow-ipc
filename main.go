package main

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/math"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func main() {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "intField", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "stringField", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "floatField", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	fmt.Println(schema.String())

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1, 0, 3, 0, 5}, []bool{true, false, true, false, true})

	rec := builder.NewRecord()
	defer rec.Release()
	slog.Info("rec:", "cols", rec.NumCols(), "rows", rec.NumRows())

	builder2 := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder2.Release()

	builder2.Field(0).(*array.Int64Builder).AppendValues([]int64{6, 7, 8, 9, 10}, nil)
	builder2.Field(1).(*array.StringBuilder).AppendValues([]string{"f", "g", "h", "i", "j"}, nil)
	builder2.Field(2).(*array.Float64Builder).AppendValues([]float64{2, 0, 6, 0, 10}, []bool{true, false, true, false, true})

	rec2 := builder2.NewRecord()
	defer rec2.Release()
	slog.Info("rec2:", "cols", rec2.NumCols(), "rows", rec2.NumRows())

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))

	err := w.Write(rec)
	if err != nil {
		slog.Error("Write failed:", "err", err)
		return
	}
	err = w.Write(rec2)
	if err != nil {
		slog.Error("Write failed:", "err", err)
		return
	}

	w.Close() // cont + 0-len meta
	fmt.Printf("[%v] %X\n", buf.Len(), buf.Bytes())

	os.WriteFile("simple.arrow", buf.Bytes(), 0644)

	// sliceAndWrite := func(rec arrow.Record, schema *arrow.Schema) {
	// 	slice := rec.NewSlice(1, 2)
	// 	defer slice.Release()

	// 	fmt.Println(slice.Columns()[0].(*array.String).Value(0))

	// 	var buf bytes.Buffer
	// 	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithZstd())
	// 	w.Write(slice)
	// 	w.Close()
	// }
	// _ = sliceAndWrite

	r, err := ipc.NewReader(&buf)
	if err != nil {
		slog.Error("NewReader failed:", "err", err)
		return
	}

	var i = -1
	defer r.Release()
	for {
		i++
		rc, err := r.Read()
		if err == io.EOF {
			slog.Info("EOF", "i", i)
			break
		}

		if err != nil {
			slog.Info("Read failed", "i", i, "err", err)
			break
		}

		slog.Info("rec:", "i", i, "cols", rc.NumCols(), "rows", rc.NumRows())
		tbl := array.NewTableFromRecords(schema, []arrow.Record{rc})
		sum := math.Float64.Sum(tbl.Column(2).Data().Chunk(0).(*array.Float64))
		slog.Info("dbg:", "sum", sum)
		tbl.Release()
	}
}
