package store

type HeartbeatStore struct {
	checkpoints *Checkpoints
}

func NewHeartbeatStore(checkpoints *Checkpoints) *HeartbeatStore {
	return &HeartbeatStore{checkpoints: checkpoints}
}

func (s *HeartbeatStore) Record(snapshot HeartbeatSnapshot) error {
	snapshot.SeenAt = snapshot.SeenAt.UTC()
	snapshot.StateChangedAt = snapshot.StateChangedAt.UTC()
	snapshot.LastRecoveredAt = snapshot.LastRecoveredAt.UTC()
	snapshot.LastAcceptedBeatAt = snapshot.LastAcceptedBeatAt.UTC()
	return s.checkpoints.SaveHeartbeat(snapshot)
}

func (s *HeartbeatStore) Load(stationID string) (HeartbeatSnapshot, bool, error) {
	value, found, err := s.checkpoints.LoadHeartbeat(stationID)
	if err != nil || !found {
		return value, found, err
	}
	value.SeenAt = value.SeenAt.UTC()
	value.StateChangedAt = value.StateChangedAt.UTC()
	value.LastRecoveredAt = value.LastRecoveredAt.UTC()
	value.LastAcceptedBeatAt = value.LastAcceptedBeatAt.UTC()
	return value, true, nil
}
