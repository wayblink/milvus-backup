# milvus-backup

## Code structure

`internal` contains codes copied from milvus project.
Keep the file structure consistent with milvus.
Some minor adjustment are made for simplicity. 

`core` contains the backup tool logic.

## DEVELOPMENT

milvus-backup and milvus-go-sdk both contain milvus.proto.
It will throw error while running UTs. Set environment to enable UT.
```
GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn
```
