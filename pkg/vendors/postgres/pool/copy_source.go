// Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved.

package pool

import "github.com/gsoultan/nextar/pkg/gpool"

// copySource is a gpool.CopyRows as caduceus reads it, with each row's
// driver.Valuers resolved the way query arguments are.
type copySource struct {
	rows gpool.CopyRows
}

func (s copySource) Next() bool { return s.rows.Next() }

func (s copySource) Values() ([]any, error) {
	values, err := s.rows.Values()
	if err != nil {
		return nil, err
	}
	return bindable(values)
}

func (s copySource) Err() error { return s.rows.Err() }
