package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	recordlayerpb "github.com/endocrimes/protoc-gen-go-fdb/api/gofdbrecordlayer/v1"
)

type Field struct {
	Name string
	Type string
}

type SecondaryIndex struct {
	Fields []Field
	Unique bool
}

type Message struct {
	Name                    string
	Fields                  []Field
	PrimaryKeyFields        []Field
	SecondaryIndexes        []SecondaryIndex
	GoPackagePath           string
	GoPackageName           protogen.GoPackageName
	GeneratedFilenamePrefix string
}

func main() {
	config := protogen.Options{}
	config.Run(func(plugin *protogen.Plugin) error {
		plugin.SupportedFeatures = uint64(
			pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL |
				pluginpb.CodeGeneratorResponse_FEATURE_SUPPORTS_EDITIONS,
		)
		plugin.SupportedEditionsMinimum = descriptorpb.Edition_EDITION_LEGACY
		plugin.SupportedEditionsMaximum = descriptorpb.Edition_EDITION_MAX

		// Extract all the required information from protobufs in order to
		// generate code.
		messages := []Message{}
		for _, file := range plugin.Files {
			if !file.Generate {
				continue
			}

			msgs := messagesFromFile(file)
			messages = append(messages, msgs...)
		}

		// Generate code for each message
		tmpl := template.Must(template.New("fdb").Funcs(template.FuncMap{
			"joinFieldNames": joinFieldNames,
		}).Parse(fdbTemplate))

		for _, msg := range messages {
			// Create a new generated file
			filename := fmt.Sprintf("%s_%s_fdb.pb.go", msg.GeneratedFilenamePrefix, strings.ToLower(msg.Name))
			genFile := plugin.NewGeneratedFile(filename, protogen.GoImportPath(msg.GoPackagePath))

			err := tmpl.Execute(genFile, msg)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Generated %s\n", filename)
		}
		return nil
	})
}

func messagesFromFile(file *protogen.File) []Message {
	var messages []Message
	goPackagePath := string(file.GoImportPath)

	for _, message := range file.Messages {
		msgName := message.GoIdent.GoName
		msgOptions := message.Desc.Options()
		processedMessage, err := processMessage(message, msgOptions)
		if err != nil {
			log.Fatalf("Unprocessable message %q: %v", msgName, err)
		}
		if processedMessage != nil {
			processedMessage.GoPackagePath = goPackagePath
			processedMessage.GoPackageName = file.GoPackageName
			processedMessage.GeneratedFilenamePrefix = file.GeneratedFilenamePrefix
			messages = append(messages, *processedMessage)
		}
	}

	return messages
}

func processMessage(message *protogen.Message, msgOptions proto.Message) (*Message, error) {
	if !proto.HasExtension(msgOptions, recordlayerpb.E_Options) {
		return nil, nil
	}
	options := proto.GetExtension(msgOptions,
		recordlayerpb.E_Options).(*recordlayerpb.MessageOptions)
	if options == nil {
		// If no message options have been specified, the message hasn't been
		// opted-in to FDB generation.
		return nil, nil
	}

	primaryKey := options.PrimaryKey

	// Require a primary key to be specified for a message to be processed.
	if len(primaryKey) == 0 {
		return nil, nil
	}

	fieldMap := make(map[string]*protogen.Field)
	fields := make([]Field, 0, len(message.Fields))

	for _, field := range message.Fields {
		fieldName := field.Desc.Name()
		fieldMap[string(fieldName)] = field
		fields = append(fields, Field{
			Name: field.GoName,
			Type: goType(field.Desc.Kind()),
		})
	}

	primaryKeyFields, err := protoFieldsToGenFields(primaryKey, fieldMap)
	if err != nil {
		return nil, fmt.Errorf("failed to generate primary index: %w", err)
	}

	secondaryIndexes := options.SecondaryIndexes
	var secondaryIndexFields []SecondaryIndex
	for _, si := range secondaryIndexes {
		fields, err := protoFieldsToGenFields(si.Fields, fieldMap)
		if err != nil {
			return nil, fmt.Errorf("failed to generate secondary index: %w", err)
		}
		secondaryIndexFields = append(secondaryIndexFields, SecondaryIndex{
			Unique: si.Unique,
			Fields: fields,
		})
	}

	return &Message{
		Name:             message.GoIdent.GoName,
		Fields:           fields,
		PrimaryKeyFields: primaryKeyFields,
		SecondaryIndexes: secondaryIndexFields,
	}, nil
}

func protoFieldsToGenFields(fields []string, fieldMap map[string]*protogen.Field) ([]Field, error) {
	result := make([]Field, 0, len(fields))
	for _, key := range fields {
		field, ok := fieldMap[key]
		if ok {
			result = append(result, Field{
				Name: field.GoName,
				Type: goType(field.Desc.Kind()),
			})
		} else {
			return nil, fmt.Errorf("required index key not found in message: %q", key)
		}
	}

	return result, nil
}

func goType(kind protoreflect.Kind) string {
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Uint32Kind:
		return "uint32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.Uint64Kind:
		return "uint64"
	case protoreflect.FloatKind:
		return "float32"
	case protoreflect.DoubleKind:
		return "float64"
	case protoreflect.BytesKind:
		return "[]byte"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BoolKind:
		return "bool"
	default:
		return "interface{}"
	}
}

func joinFieldNames(fields []Field) string {
	names := []string{}
	for _, f := range fields {
		names = append(names, f.Name)
	}
	return strings.Join(names, "And")
}

const fdbTemplate = `package {{.GoPackageName}}

import (
    "context"
    "fmt"

    "github.com/apple/foundationdb/bindings/go/src/fdb"
		"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
    "github.com/apple/foundationdb/bindings/go/src/fdb/directory"
		"google.golang.org/protobuf/proto"
)

type {{.Name}}Store struct {
    db  fdb.Database
    dir directory.DirectorySubspace
}

func New{{.Name}}Store(db fdb.Database) (*{{.Name}}Store, error) {
    dir, err := directory.CreateOrOpen(db, []string{"gorecords", "{{.Name}}"}, nil)
    if err != nil {
        return nil, err
    }
    return &{{.Name}}Store{db: db, dir: dir}, nil
}

func (store *{{.Name}}Store) Get(ctx context.Context, tr fdb.ReadTransaction, {{range $index, $element := .PrimaryKeyFields}}{{if $index}}, {{end}}{{.Name}} {{.Type}}{{end}}) (*{{.Name}}, error) {
    var entity *{{.Name}}

    key := store.dir.Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} {{.Name}}, {{end}} })
    value, err := tr.Get(key).Get()
		if err != nil {
				return nil, fmt.Errorf("failed to get value: %w", err)
		}
    if value == nil {
        return nil, nil
    }
    entity = &{{.Name}}{}
    err = proto.Unmarshal(value, entity)
    if err != nil {
        return nil, err
    }
    return entity, nil
}

func (store *{{.Name}}Store) Set(ctx context.Context, tr fdb.Transaction, entity *{{.Name}}) error {
    key := store.dir.Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} entity.{{.Name}}, {{end}} })
    value, err := proto.Marshal(entity)
    if err != nil {
        return err
    }

    {{range $idxIndex, $idx := .SecondaryIndexes}}
		// Setup {{joinFieldNames $idx.Fields}} index
    indexKey {{if eq $idxIndex 0}}:{{end}}= store.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
        {{range $i, $f := $idx.Fields}} entity.{{ $f.Name }}, {{end}}
        {{range $.PrimaryKeyFields}} entity.{{.Name}}, {{end}}
    })
    tr.Set(indexKey, []byte{})

    {{end}}

		// Set the primary key value
    tr.Set(key, value)

    return nil
}

func (store *{{.Name}}Store) Delete(ctx context.Context, tr fdb.Transaction, {{range $index, $element := .PrimaryKeyFields}}{{if $index}}, {{end}}{{.Name}} {{.Type}}{{end}}) error {
    key := store.dir.Pack(tuple.Tuple{ {{range .PrimaryKeyFields}} {{.Name}}, {{end}} })
		value, err := tr.Get(key).Get()
		if err != nil {
			return err
		}
		if value == nil {
			return nil
		}

		entity := &{{.Name}}{}
		err = proto.Unmarshal(value, entity)
		if err == nil {
				{{range $idxIndex, $idx := .SecondaryIndexes}}
				// Cleanup {{joinFieldNames $idx.Fields}} index
				indexKey {{if eq $idxIndex 0 }}:{{end}}= store.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{
						{{range $i, $f := $idx.Fields}} entity.{{ $f.Name }}, {{end}}
						{{range $.PrimaryKeyFields}} entity.{{.Name}}, {{end}}
				})
				tr.Clear(indexKey)
				{{end}}
		}

    tr.Clear(key)
    return nil
}

{{/* Generate GetBy functions for non-unique secondary indexes */}}
{{range $idxIndex, $idx := .SecondaryIndexes}}
{{ if $idx.Unique }}
{{ continue }}
{{ end }}
func (store *{{$.Name}}Store) GetBy{{joinFieldNames $idx.Fields}}(ctx context.Context, tr fdb.ReadTransaction, {{range $i, $f := $idx.Fields}}{{if $i}}, {{end}}{{$f.Name}} {{$f.Type}}{{end}}) ([]*{{$.Name}}, error) {
    entities := []*{{$.Name}}{}
    indexKeyPrefix := store.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{ {{range $i, $f := $idx.Fields}} {{$f.Name}}, {{end}} })
    indexRange, err := fdb.PrefixRange(indexKeyPrefix)
		if err != nil {
			return nil, err
		}

    kvs, err := tr.GetRange(indexRange, fdb.RangeOptions{}).GetSliceWithError()
		if err != nil {
			return nil, err
		}

    for _, kv := range kvs {
        indexTuple, err := store.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Unpack(kv.Key)
        if err != nil {
            return nil, err
        }

				// The index key layout is [(secondary index)(primary key)]
				// So unpack the key tuple and skip past the secondary index to get
				// the primary key.
        pkTuple := indexTuple[{{len $idx.Fields}}:]
        key := store.dir.Pack(pkTuple)
				value, err := tr.Get(key).Get()
				if err != nil {
						return nil, fmt.Errorf("failed to get value: %w", err)
				}
        if value == nil {
            continue
        }

        entity := &{{$.Name}}{}
        err = proto.Unmarshal(value, entity)
        if err != nil {
            return nil, err
        }

        entities = append(entities, entity)
    }
    return entities, nil
}
{{end}}

{{/* Generate GetBy functions for unique secondary indexes */}}
{{range $idxIndex, $idx := .SecondaryIndexes}}
{{ if not $idx.Unique }}
{{ continue }}
{{ end }}
func (store *{{$.Name}}Store) GetBy{{joinFieldNames $idx.Fields}}(ctx context.Context, tr fdb.ReadTransaction, {{range $i, $f := $idx.Fields}}{{if $i}}, {{end}}{{$f.Name}} {{$f.Type}}{{end}}) (*{{$.Name}}, error) {
    indexKeyPrefix := store.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Pack(tuple.Tuple{ {{range $i, $f := $idx.Fields}} {{$f.Name}}, {{end}} })
    indexRange, err := fdb.PrefixRange(indexKeyPrefix)
		if err != nil {
			return nil, err
		}

    kvs, err := tr.GetRange(indexRange, fdb.RangeOptions{}).GetSliceWithError()
		if err != nil {
			return nil, err
		}

		if len(kvs) == 0 {
			return nil, nil
		}

		if len(kvs) > 1 {
			return nil, fmt.Errorf("unexpected number of values for index, expected 1, got: %d", len(kvs))
		}

		kv := kvs[0]

		indexTuple, err := store.dir.Sub("{{joinFieldNames $idx.Fields}}_index").Unpack(kv.Key)
		if err != nil {
				return nil, err
		}

		// The index key layout is [(secondary index)(primary key)]
		// So unpack the key tuple and skip past the secondary index to get
		// the primary key.
		pkTuple := indexTuple[{{len $idx.Fields}}:]
		key := store.dir.Pack(pkTuple)
		value, err := tr.Get(key).Get()
		if err != nil {
				return nil, fmt.Errorf("failed to get value: %w", err)
		}
		if value == nil {
				return nil, nil
		}

    entity := &{{$.Name}}{}
		err = proto.Unmarshal(value, entity)
		if err != nil {
				return nil, err
		}
		return entity, nil
}
{{end}}
`
