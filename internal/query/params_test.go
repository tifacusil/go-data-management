package query

import (
	"errors"
	"net/url"
	"testing"
)

func vals(m map[string]string) url.Values {
	v := url.Values{}
	for k, val := range m {
		v.Set(k, val)
	}
	return v
}

func TestParseParams_Defaults(t *testing.T) {
	p, err := ParseParams(vals(nil), true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if p.Limit != defaultLimit || p.Offset != 0 {
		t.Errorf("defaults: limit=%d offset=%d", p.Limit, p.Offset)
	}
	if p.Filter.SensorType != nil || p.Filter.From != nil {
		t.Error("filtros deben ser nil por defecto")
	}
}

func TestParseParams_Errors(t *testing.T) {
	cases := map[string]map[string]string{
		"limit cero":          {"limit": "0"},
		"limit excede":        {"limit": "1001"},
		"offset negativo":     {"offset": "-1"},
		"sensor_type malo":    {"sensor_type": "lava"},
		"from > to":           {"from": "2026-05-18T00:00:00Z", "to": "2026-05-17T00:00:00Z"},
		"fix_quality no gnss": {"sensor_type": "rainfall", "fix_quality": "RTK"},
		"from no iso":         {"from": "ayer"},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseParams(vals(m), true)
			var bad *BadRequest
			if !errors.As(err, &bad) {
				t.Errorf("%s: esperaba BadRequest, got %v", name, err)
			}
		})
	}
}

func TestParseParams_FixQualityWithGnssOK(t *testing.T) {
	p, err := ParseParams(vals(map[string]string{"sensor_type": "gnss", "fix_quality": "RTK_FIXED"}), true)
	if err != nil {
		t.Fatalf("gnss+fix_quality debe ser valido: %v", err)
	}
	if p.Filter.FixQuality == nil || *p.Filter.FixQuality != "RTK_FIXED" {
		t.Error("fix_quality no parseado")
	}
}

func TestParseParams_LimitMaxOK(t *testing.T) {
	p, err := ParseParams(vals(map[string]string{"limit": "1000"}), true)
	if err != nil || p.Limit != 1000 {
		t.Errorf("limit=1000 debe ser valido, got limit=%d err=%v", p.Limit, err)
	}
}
