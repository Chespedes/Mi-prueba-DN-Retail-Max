{
    "name": "PL_retailmax_medallion",
    "objectId": "4eb8157a-2ae5-4038-9dc2-8afc264ae4ff",
    "properties": {
        "activities": [
            {
                "name": "nb_02_silver",
                "type": "TridentNotebook",
                "dependsOn": [
                    {
                        "activity": "Inv_pl_bronze",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "notebookId": "44f8f201-558e-45da-8175-f166b4646e69",
                    "workspaceId": "b790acba-7ed8-401e-9ac6-dbaa256bddd8"
                },
                "externalReferences": {
                    "connection": "e70b88ab-9e80-430e-bd6c-095ac7fa6966"
                }
            },
            {
                "name": "nb_03_gold",
                "type": "TridentNotebook",
                "dependsOn": [
                    {
                        "activity": "nb_02_silver",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "notebookId": "f01fffc1-ad52-4e6d-bfe0-2a1db59bd750",
                    "workspaceId": "b790acba-7ed8-401e-9ac6-dbaa256bddd8"
                }
            },
            {
                "name": "Inv_pl_bronze",
                "type": "InvokePipeline",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "waitOnCompletion": true,
                    "operationType": "InvokeFabricPipeline",
                    "pipelineId": "3c760afe-ad6c-4d14-8024-635507d02392",
                    "workspaceId": "b790acba-7ed8-401e-9ac6-dbaa256bddd8"
                },
                "externalReferences": {
                    "connection": "90d4afe7-3391-4330-9e99-72141cacdf3b"
                }
            }
        ],
        "lastModifiedByObjectId": "6a3ca537-5785-4d15-932f-ffc19b1d2886",
        "lastPublishTime": "2026-07-26T21:14:14Z"
    }
}
