package storage

import (
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// groups maps group URIs, without the scheme, to short names.
var groups = map[string]string{
	"acs.amazonaws.com/groups/global/AllUsers":           "AllUsers",
	"acs.amazonaws.com/groups/global/AuthenticatedUsers": "AuthenticatedUsers",
}

// cannedByGrants maps the grants besides the owner's FULL_CONTROL, written as
// sorted "<group>:<permission>" strings, to the canned ACL.
var cannedByGrants = map[string]string{
	"":                             "private",
	"AllUsers:READ":                "public-read",
	"AllUsers:READ,AllUsers:WRITE": "public-read-write",
	"AuthenticatedUsers:READ":      "authenticated-read",
}

// CannedACL returns the canned ACL equivalent to grants, or ok == false. The
// owner is recognised by its id, whatever the provider's id format is.
func CannedACL(owner string, grants []types.Grant) (acl string, ok bool) {
	var rest []string
	ownerFull := false
	for _, g := range grants {
		if g.Grantee == nil {
			return "", false
		}
		if g.Grantee.Type == types.TypeCanonicalUser && aws.ToString(g.Grantee.ID) == owner {
			if g.Permission == types.PermissionFullControl {
				ownerFull = true
				continue
			}
			return "", false
		}
		if g.Grantee.Type != types.TypeGroup {
			return "", false
		}
		uri := aws.ToString(g.Grantee.URI)
		uri = strings.TrimPrefix(strings.TrimPrefix(uri, "http://"), "https://")
		group, known := groups[uri]
		if !known {
			return "", false
		}
		rest = append(rest, group+":"+string(g.Permission))
	}
	if !ownerFull {
		return "", false
	}
	sort.Strings(rest)
	acl, ok = cannedByGrants[strings.Join(rest, ",")]
	return acl, ok
}
