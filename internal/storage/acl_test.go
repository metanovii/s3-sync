package storage

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

func owner(p types.Permission) types.Grant {
	return types.Grant{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String("me")}, Permission: p}
}

func group(uri string, p types.Permission) types.Grant {
	return types.Grant{Grantee: &types.Grantee{Type: types.TypeGroup, URI: aws.String(uri)}, Permission: p}
}

const allUsers = "http://acs.amazonaws.com/groups/global/AllUsers"

func TestCannedACL(t *testing.T) {
	full := owner(types.PermissionFullControl)
	tests := []struct {
		name   string
		grants []types.Grant
		want   string
		ok     bool
	}{
		{"private", []types.Grant{full}, "private", true},
		{"public-read", []types.Grant{full, group(allUsers, types.PermissionRead)}, "public-read", true},
		{"public-read https uri", []types.Grant{group("https://acs.amazonaws.com/groups/global/AllUsers", types.PermissionRead), full}, "public-read", true},
		{"public-read-write", []types.Grant{full, group(allUsers, types.PermissionWrite), group(allUsers, types.PermissionRead)}, "public-read-write", true},
		{"authenticated-read", []types.Grant{full, group("http://acs.amazonaws.com/groups/global/AuthenticatedUsers", types.PermissionRead)}, "authenticated-read", true},
		{"no owner grant", []types.Grant{group(allUsers, types.PermissionRead)}, "", false},
		{"other user", []types.Grant{full, {Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: aws.String("other")}, Permission: types.PermissionRead}}, "", false},
		{"unknown group", []types.Grant{full, group("http://acs.amazonaws.com/groups/s3/LogDelivery", types.PermissionWrite)}, "", false},
		{"owner read only", []types.Grant{owner(types.PermissionRead)}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := CannedACL("me", tt.grants)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}
