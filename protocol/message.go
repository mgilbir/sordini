package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

type MessagePayload struct {
	Offset int64
	Key    []byte
	Value  []byte
}

func ExtractMessagePayload(in []byte) ([]MessagePayload, error) {
	var ret []MessagePayload

	switch magic := in[16]; magic {
	case 0:
		m := new(kmsg.MessageV0)
		if err := m.ReadFrom(in); err != nil {
			return nil, err
		}

		ret = append(ret, MessagePayload{
			Offset: m.Offset,
			Key:    m.Key,
			Value:  m.Value,
		})

	case 1:
		m := new(kmsg.MessageV1)
		if err := m.ReadFrom(in); err != nil {
			return nil, err
		}

		ret = append(ret, MessagePayload{
			Offset: m.Offset,
			Key:    m.Key,
			Value:  m.Value,
		})

	case 2:
		rb := new(kmsg.RecordBatch)
		if err := rb.ReadFrom(in); err != nil {
			return nil, err
		}

		records, err := parseRecordBatch(rb.Records)
		if err != nil {
			return nil, err
		}

		for _, r := range records {
			ret = append(ret, MessagePayload{
				Offset: int64(rb.FirstOffset) + int64(r.OffsetDelta),
				Key:    r.Key,
				Value:  r.Value,
			})
		}

	default:
		return nil, fmt.Errorf("unknown magic %d", magic)
	}

	return ret, nil
}

func parseRecordBatch(in []byte) ([]kmsg.Record, error) {
	var ret []kmsg.Record

	for len(in) > 4 {
		l := int32(binary.BigEndian.Uint32(in[:4]))
		r := new(kmsg.Record)
		if err := r.ReadFrom(in[:l]); err != nil {
			return nil, err
		}

		ret = append(ret, *r)
		in = in[l:]
	}

	return ret, nil
}
