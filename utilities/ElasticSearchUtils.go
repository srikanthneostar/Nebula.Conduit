package utilities

func GetEventsFilterCondition(lastEventId int) map[string]interface{} {
	return map[string]interface{}{
		"bool": map[string]interface{}{
			"must": []map[string]interface{}{
				{
					"match": map[string]interface{}{
						"entityId": 60,
					},
				},
				{
					"range": map[string]interface{}{
						"entity.id": map[string]interface{}{
							"gt": lastEventId,
						},
					},
				},
			},
		},
	}
}
