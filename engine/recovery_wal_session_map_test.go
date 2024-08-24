package engine

import (
	"testing"

	"github.com/go-streamline/core/repo"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestCreateSessionIDToLastEntryMap(t *testing.T) {
	tests := []struct {
		name           string
		entries        []repo.LogEntry
		expectedOutput map[uuid.UUID]repo.LogEntry
	}{
		{
			name: "Single entry",
			entries: []repo.LogEntry{
				{
					SessionID:     uuid.MustParse("1bfcf809-7cc4-4834-bb5e-33c37a66c126"),
					ProcessorName: "handler1",
					ProcessorID:   "handler1",
				},
			},
			expectedOutput: map[uuid.UUID]repo.LogEntry{
				uuid.MustParse("1bfcf809-7cc4-4834-bb5e-33c37a66c126"): {
					SessionID:     uuid.MustParse("1bfcf809-7cc4-4834-bb5e-33c37a66c126"),
					ProcessorName: "handler1",
					ProcessorID:   "handler1",
				},
			},
		},
		{
			name: "Multiple entries, same session",
			entries: []repo.LogEntry{
				{
					SessionID:     uuid.MustParse("da3f59f7-83b1-43a9-9465-9312575839f6"),
					ProcessorName: "handler1",
					ProcessorID:   "handler1",
				},
				{
					SessionID:     uuid.MustParse("da3f59f7-83b1-43a9-9465-9312575839f6"),
					ProcessorName: "handler2",
					ProcessorID:   "handler2",
				},
			},
			expectedOutput: map[uuid.UUID]repo.LogEntry{
				uuid.MustParse("da3f59f7-83b1-43a9-9465-9312575839f6"): {
					SessionID:     uuid.MustParse("da3f59f7-83b1-43a9-9465-9312575839f6"),
					ProcessorName: "handler2",
					ProcessorID:   "handler2",
				},
			},
		},
		{
			name: "Multiple entries, different sessions",
			entries: []repo.LogEntry{
				{
					SessionID:     uuid.MustParse("8b8c96b7-d0e7-4bed-9d21-db1f85661039"),
					ProcessorName: "handler1",
					ProcessorID:   "handler1",
				},
				{
					SessionID:     uuid.MustParse("82158378-9bd0-4adb-be50-da697c58c675"),
					ProcessorName: "handler2",
					ProcessorID:   "handler2",
				},
				{
					SessionID:     uuid.MustParse("75565bad-1dce-4565-825d-2e3831a57a4f"),
					ProcessorName: "handler3",
					ProcessorID:   "handler3",
				},
			},
			expectedOutput: map[uuid.UUID]repo.LogEntry{
				uuid.MustParse("8b8c96b7-d0e7-4bed-9d21-db1f85661039"): {
					SessionID:     uuid.MustParse("8b8c96b7-d0e7-4bed-9d21-db1f85661039"),
					ProcessorName: "handler1",
					ProcessorID:   "handler1",
				},
				uuid.MustParse("82158378-9bd0-4adb-be50-da697c58c675"): {
					SessionID:     uuid.MustParse("82158378-9bd0-4adb-be50-da697c58c675"),
					ProcessorName: "handler2",
					ProcessorID:   "handler2",
				},
				uuid.MustParse("75565bad-1dce-4565-825d-2e3831a57a4f"): {
					SessionID:     uuid.MustParse("75565bad-1dce-4565-825d-2e3831a57a4f"),
					ProcessorName: "handler3",
					ProcessorID:   "handler3",
				},
			},
		},
		{
			name: "Session marked as ended",
			entries: []repo.LogEntry{
				{
					SessionID:     uuid.MustParse("8b8c96b7-d0e7-4bed-9d21-db1f85661039"),
					ProcessorName: "handler1",
					ProcessorID:   "handler1",
				},
				{
					SessionID:     uuid.MustParse("8b8c96b7-d0e7-4bed-9d21-db1f85661039"),
					ProcessorName: "__end__",
					ProcessorID:   "handler2",
				},
			},
			expectedOutput: map[uuid.UUID]repo.LogEntry{},
		},
		{
			name: "Mixed entries with ended session",
			entries: []repo.LogEntry{
				{
					SessionID:     uuid.MustParse("91faadb6-fbe2-4952-a189-f9e7d4e3badd"),
					ProcessorName: "handler1",
					ProcessorID:   "handler1",
				},
				{
					SessionID:     uuid.MustParse("91faadb6-fbe2-4952-a189-f9e7d4e3badd"),
					ProcessorName: "handler2",
					ProcessorID:   "handler2",
				},
				{
					SessionID:     uuid.MustParse("91faadb6-fbe2-4952-a189-f9e7d4e3badd"),
					ProcessorName: "__end__",
					ProcessorID:   "handler3",
				},
			},
			expectedOutput: map[uuid.UUID]repo.LogEntry{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := &Engine{}
			output := engine.createSessionIDToLastEntryMap(tt.entries)

			for sessionID, expectedEntry := range tt.expectedOutput {
				actualEntry, exists := output[sessionID]
				assert.True(t, exists)
				assert.Equal(t, expectedEntry.SessionID, actualEntry.SessionID)
				assert.Equal(t, expectedEntry.ProcessorName, actualEntry.ProcessorName)
				assert.Equal(t, expectedEntry.ProcessorID, actualEntry.ProcessorID)
			}

			assert.Equal(t, len(tt.expectedOutput), len(output))
		})
	}
}
