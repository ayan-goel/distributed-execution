package main

import (
	"fmt"
	"io"
	"os"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"google.golang.org/protobuf/proto"
)

func golden() *pb.Assignment {
	return &pb.Assignment{
		Authority:            &pb.AttemptAuthority{JobId: "00000000-0000-0000-0000-000000000001", AttemptId: "00000000-0000-0000-0000-000000000002", WorkerId: "00000000-0000-0000-0000-000000000003", SessionId: "00000000-0000-0000-0000-000000000004", Generation: 9007199254740993},
		ImageDigest:          "example.org/eval@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Argv:                 []string{"python", "evaluate.py", "--label", "λ"},
		Resources:            &pb.Resources{CpuMillis: 2000, MemoryBytes: 4294967296, ScratchBytes: 8589934592},
		CanonicalJobSpecJson: []byte(`{"kind":"Job"}`),
		SpecSha256:           "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		LeaseDurationMs:      25000,
		Inputs: []*pb.InputManifest{{
			DatasetId: "00000000-0000-0000-0000-000000000005", MountPath: "/inputs/example", DatasetName: "example",
			Archive: &pb.ObjectVersion{Key: "projects/p/datasets/archive", VersionId: "v1", SizeBytes: 4096,
				Sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
			FileManifestJson: []byte(`{"format":"tar.v1","files":[{"path":"data.txt","sizeBytes":4,"sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}]}`),
			DownloadUrl:      "https://storage.example.test/projects/p/datasets/archive?versionId=v1&signature=secret",
			ExpiresUnixMs:    1780000000000,
		}},
	}
}

func main() {
	if len(os.Args) != 2 {
		panic("expected create or check")
	}
	var expected proto.Message = golden()
	var actual proto.Message = &pb.Assignment{}
	if os.Args[1] == "create-page" || os.Args[1] == "check-page" {
		expected = &pb.ListAssignmentsResponse{Assignments: []*pb.Assignment{golden()}, NextAfterJobId: "00000000-0000-0000-0000-000000000001"}
		actual = &pb.ListAssignmentsResponse{}
	}
	switch os.Args[1] {
	case "create", "create-page":
		b, err := proto.Marshal(expected)
		if err != nil {
			panic(err)
		}
		if _, err = os.Stdout.Write(b); err != nil {
			panic(err)
		}
	case "check", "check-page":
		b, err := io.ReadAll(io.LimitReader(os.Stdin, 4<<20))
		if err != nil {
			panic(err)
		}
		if err = proto.Unmarshal(b, actual); err != nil {
			panic(err)
		}
		if !proto.Equal(expected, actual) {
			panic(fmt.Sprintf("round-trip changed fields: %v", &actual))
		}
	default:
		panic("expected create or check")
	}
}
