package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AngelloFD/go-data-management/internal/model"
	"github.com/AngelloFD/go-data-management/internal/persistence"
)

type spyRepo struct {
	gnss, rainfall, strain, soil, deep int
	err                                error
}

func (s *spyRepo) PersistGNSS(_ context.Context, _ *model.Envelope, _ *model.GNSSPayload) error {
	s.gnss++
	return s.err
}
func (s *spyRepo) PersistRainfall(_ context.Context, _ *model.Envelope, _ *model.RainfallPayload) error {
	s.rainfall++
	return s.err
}
func (s *spyRepo) PersistStrain(_ context.Context, _ *model.Envelope, _ *model.StrainPayload) error {
	s.strain++
	return s.err
}
func (s *spyRepo) PersistSoilPressure(_ context.Context, _ *model.Envelope, _ *model.SoilPressurePayload) error {
	s.soil++
	return s.err
}
func (s *spyRepo) PersistDeepDisplacement(_ context.Context, _ *model.Envelope, _ *model.DeepDisplacementPayload) error {
	s.deep++
	return s.err
}

func env(st model.SensorType, payload string) *model.Envelope {
	return &model.Envelope{
		MessageID:  "m1",
		SensorType: st,
		Timestamp:  time.Now(),
		ReceivedAt: time.Now(),
		Payload:    []byte(payload),
	}
}

func TestRoute_DispatchesBySensorType(t *testing.T) {
	cases := []struct {
		st      model.SensorType
		payload string
		check   func(*spyRepo) int
	}{
		{model.SensorGNSS, `{"displacement_x_mm":1}`, func(s *spyRepo) int { return s.gnss }},
		{model.SensorRainfall, `{"rainfall_mm":1}`, func(s *spyRepo) int { return s.rainfall }},
		{model.SensorStrain, `{"strain_microstrain":1}`, func(s *spyRepo) int { return s.strain }},
		{model.SensorSoilPressure, `{"pressure_kpa":1}`, func(s *spyRepo) int { return s.soil }},
		{model.SensorDeepDisplacement, `{"depth_m":1}`, func(s *spyRepo) int { return s.deep }},
	}
	for _, c := range cases {
		t.Run(string(c.st), func(t *testing.T) {
			repo := &spyRepo{}
			r := New(repo)
			if err := r.Route(context.Background(), env(c.st, c.payload)); err != nil {
				t.Fatalf("route: %v", err)
			}
			if c.check(repo) != 1 {
				t.Errorf("%s no enrutado", c.st)
			}
		})
	}
}

func TestRoute_UnsupportedTypeIsNonRetryable(t *testing.T) {
	r := New(&spyRepo{})
	err := r.Route(context.Background(), env("lava", `{}`))
	var nre *persistence.NonRetryableError
	if !errors.As(err, &nre) {
		t.Fatalf("tipo no soportado debe ser NonRetryableError, got %T", err)
	}
}

func TestRoute_BadPayloadIsNonRetryable(t *testing.T) {
	r := New(&spyRepo{})
	err := r.Route(context.Background(), env(model.SensorGNSS, `{not json`))
	var nre *persistence.NonRetryableError
	if !errors.As(err, &nre) {
		t.Fatalf("payload roto debe ser NonRetryableError, got %T", err)
	}
}

func TestRoute_RepoErrorPassesThrough(t *testing.T) {
	repo := &spyRepo{err: &persistence.RetryableError{Err: errors.New("deadlock")}}
	r := New(repo)
	err := r.Route(context.Background(), env(model.SensorRainfall, `{"rainfall_mm":1}`))
	var re *persistence.RetryableError
	if !errors.As(err, &re) {
		t.Fatalf("error de repo debe propagarse clasificado, got %T", err)
	}
}
