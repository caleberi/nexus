package server

type CreatePluginRequest struct {
	Name        string `json:"name"`
	Code        string `json:"code"`
	Description string `json:"description"`
}

type GetPluginReueust struct {
	Name string `json:"name"`
}

type SubmitEventRequest struct {
	DelegationType string `json:"delegationType"`
	Payload        string `json:"payload"`
	MaxAttempts    int    `json:"maxAttempts"`
	Priority       int    `json:"priority"`
}
