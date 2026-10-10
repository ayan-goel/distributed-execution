package main

import (
	"strings"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"google.golang.org/protobuf/proto"
)

func multipartGolden(kind string) proto.Message {
	const uploadID = "00000000-0000-0000-0000-000000000006"
	const requestID = "00000000-0000-0000-0000-000000000007"
	const key = "projects/p/jobs/j/attempts/a/outputs/result"
	const size = (5 << 20) + 3
	hash := strings.Repeat("a", 64)
	switch kind {
	case "upload-request":
		return &pb.CreateUploadRequest{Authority: golden().Authority, RequestId: requestID, Name: "result", Kind: pb.ArtifactKind_OUTPUT, SizeBytes: size, Sha256: hash, PartCount: 2, PartSizeBytes: 5 << 20}
	case "upload-response":
		return &pb.CreateUploadResponse{UploadId: uploadID, ObjectKey: key, PartCount: 2, PartSizeBytes: 5 << 20}
	case "part-request":
		return &pb.GrantUploadPartRequest{Authority: golden().Authority, UploadId: uploadID, Number: 2, Sha256: hash}
	case "part-response":
		return &pb.UploadPart{Number: 2, Url: "https://storage.example.test/part?signature=fixture", RequiredHeaders: map[string]string{"x-amz-checksum-sha256": "fixture-checksum", "Content-Length": "3"}, ExpiresUnixMs: 1780000000000}
	case "finalize":
		return &pb.FinalizeUploadRequest{Authority: golden().Authority, RequestId: requestID, UploadId: uploadID, Object: &pb.ObjectVersion{Key: key, SizeBytes: size, Sha256: hash}, Parts: []*pb.CompletedPart{{Number: 1, Etag: `"first"`, Sha256: hash}, {Number: 2, Etag: `"last"`, Sha256: strings.Repeat("b", 64)}}}
	default:
		return nil
	}
}
