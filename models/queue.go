package models

type QueueInfo struct {
	Name   string `json:"name"`
	Depth  int64  `json:"depth"`
	Paused bool   `json:"paused"`
}

type QueueStats struct {
	Name    string `json:"name"`
	Pending int64  `json:"pending"`
	Dead    int64  `json:"dead"`
	Paused  bool   `json:"paused"`
}
