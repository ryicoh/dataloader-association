package association_test

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"

	dataloader "github.com/graph-gophers/dataloader/v7"
	association "github.com/ryicoh/dataloader-association"
)

type (
	User struct {
		ID      int
		Name    string
		GroupID int
	}

	Member struct {
		UserID  int
		GroupID int
		Order   int
		Role    string
	}

	MemberInfo struct {
		UserID int
		Name   string
		Role   string
	}
)

var (
	userData = []*User{
		{ID: 1, Name: "John", GroupID: 1},
		{ID: 2, Name: "Bob", GroupID: 1},
		{ID: 3, Name: "Alice", GroupID: 2},
		{ID: 4, Name: "Jane", GroupID: 2},
		{ID: 5, Name: "Mike", GroupID: 3},
	}

	memberData = []*Member{
		{UserID: 1, GroupID: 1, Order: 1, Role: "Admin"},
		{UserID: 1, GroupID: 2, Order: 4, Role: "Admin"},
		{UserID: 2, GroupID: 1, Order: 2, Role: ""},
		{UserID: 3, GroupID: 2, Order: 1, Role: ""},
		{UserID: 4, GroupID: 2, Order: 3, Role: ""},
		{UserID: 4, GroupID: 3, Order: 2, Role: ""},
		{UserID: 5, GroupID: 1, Order: 3, Role: ""},
		{UserID: 5, GroupID: 2, Order: 2, Role: ""},
		{UserID: 5, GroupID: 3, Order: 1, Role: ""},
	}
)

var getUsersByUserID = func(ctx context.Context, keys []int) ([]*User, error) {
	var results []*User
	for _, user := range userData {
		for _, key := range keys {
			if user.ID == key {
				results = append(results, user)
			}
		}
	}

	return results, nil
}

func TestNewHasOneBatchFunc(t *testing.T) {
	batchFn := association.NewHasOneBatchFunc[int, *User](getUsersByUserID, func(v *User) int {
		return v.ID
	})

	testCases := []struct {
		name string
		keys []int
		want map[int]*User
	}{
		{"empty", []int{}, map[int]*User{}},
		{"user 1", []int{1}, map[int]*User{1: userData[0]}},
		{"user 1,3", []int{1, 3}, map[int]*User{1: userData[0], 3: userData[2]}},
		{"user 2", []int{2}, map[int]*User{2: userData[1]}},
		{"user 10", []int{10}, map[int]*User{}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want)
		})
	}
}

var getUsersByGroupID = func(ctx context.Context, keys []int) ([]*User, error) {
	var results []*User
	for _, user := range userData {
		for _, key := range keys {
			if user.GroupID == key {
				results = append(results, user)
			}
		}
	}

	return results, nil
}

func TestNewHasManyBatchFunc(t *testing.T) {
	batchFn := association.NewHasManyBatchFunc[int, *User](getUsersByGroupID, func(v *User) int {
		return v.GroupID
	})

	testCases := []struct {
		name string
		keys []int
		want map[int][]*User
	}{
		{"empty", []int{}, map[int][]*User{}},
		{"group 1", []int{1}, map[int][]*User{1: {userData[0], userData[1]}}},
		{"group 1,3", []int{1, 3}, map[int][]*User{1: {userData[0], userData[1]}, 3: {userData[4]}}},
		{"group 1,2,3", []int{1, 2, 3}, map[int][]*User{1: {userData[0], userData[1]}, 2: {userData[2], userData[3]}, 3: {userData[4]}}},
		{"group 1,2,3,10", []int{1, 2, 3}, map[int][]*User{
			1:  {userData[0], userData[1]},
			2:  {userData[2], userData[3]},
			3:  {userData[4]},
			10: {},
		}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want)
		})
	}
}

func TestNewHasManyBatchFuncWithSort(t *testing.T) {
	batchFn := association.NewHasManyBatchFunc[int, *User](
		getUsersByGroupID, func(v *User) int {
			return v.GroupID
		},
		association.WithSort(func(users []*User) {
			sort.SliceStable(users, func(i, j int) bool {
				return users[i].ID > users[j].ID
			})
		}),
	)

	testCases := []struct {
		name string
		keys []int
		want map[int][]*User
	}{
		{"empty", []int{}, map[int][]*User{}},
		{"group 1,2,3,10", []int{1, 2, 3}, map[int][]*User{
			1:  {userData[1], userData[0]},
			2:  {userData[3], userData[2]},
			3:  {userData[4]},
			10: {},
		}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want)
		})
	}
}

var getMembersByGroupID = func(ctx context.Context, keys []int) ([]*Member, error) {
	var results []*Member
	for _, member := range memberData {
		for _, key := range keys {
			if member.GroupID == key {
				results = append(results, member)
			}
		}
	}

	return results, nil
}

func TestNewManyToManyBatchFunc(t *testing.T) {
	batchFn := association.NewManyToManyBatchFunc[int, *Member, *User, *User](
		getMembersByGroupID,
		getUsersByUserID,
		func(v *Member) int {
			return v.GroupID
		},
		func(v *Member) int {
			return v.UserID
		},
		func(v *User) int {
			return v.ID
		},
		func(_ *Member, u *User) *User {
			return u
		},
	)

	testCases := []struct {
		name string
		keys []int
		want map[int][]*User
	}{
		{"empty", []int{}, map[int][]*User{}},
		{"group 1", []int{1}, map[int][]*User{1: {userData[0], userData[1], userData[4]}}},
		{"group 1,3", []int{1, 3}, map[int][]*User{1: {userData[0], userData[1], userData[4]}, 3: {userData[3], userData[4]}}},
		{"group 1,2,3", []int{1, 2, 3}, map[int][]*User{
			1: {userData[0], userData[1], userData[4]},
			2: {userData[0], userData[2], userData[3], userData[4]},
			3: {userData[3], userData[4]}}},
		{"group 1,2,3,10", []int{1, 2, 3}, map[int][]*User{
			1:  {userData[0], userData[1], userData[4]},
			2:  {userData[0], userData[2], userData[3], userData[4]},
			3:  {userData[3], userData[4]},
			10: {},
		}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want)
		})
	}
}

func TestNewManyToManyBatchFunc2(t *testing.T) {
	batchFn := association.NewManyToManyBatchFunc[int, *Member, *User, *MemberInfo](
		getMembersByGroupID,
		getUsersByUserID,
		func(v *Member) int {
			return v.GroupID
		},
		func(v *Member) int {
			return v.UserID
		},
		func(v *User) int {
			return v.ID
		},
		func(m *Member, u *User) *MemberInfo {
			return &MemberInfo{
				UserID: u.ID,
				Name:   u.Name,
				Role:   m.Role,
			}
		},
	)

	testCases := []struct {
		name string
		keys []int
		want map[int][]*MemberInfo
	}{
		{"group 1,2,3", []int{1, 2, 3}, map[int][]*MemberInfo{
			1: {
				&MemberInfo{UserID: userData[0].ID, Name: userData[0].Name, Role: "Admin"},
				&MemberInfo{UserID: userData[1].ID, Name: userData[1].Name},
				&MemberInfo{UserID: userData[4].ID, Name: userData[4].Name},
			},
			2: {
				&MemberInfo{UserID: userData[0].ID, Name: userData[0].Name, Role: "Admin"},
				&MemberInfo{UserID: userData[2].ID, Name: userData[2].Name},
				&MemberInfo{UserID: userData[3].ID, Name: userData[3].Name},
				&MemberInfo{UserID: userData[4].ID, Name: userData[4].Name},
			},
			3: {
				&MemberInfo{UserID: userData[3].ID, Name: userData[3].Name},
				&MemberInfo{UserID: userData[4].ID, Name: userData[4].Name},
			},
		}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want)
		})
	}
}

func TestNewManyToManyBatchFuncWithSort(t *testing.T) {
	batchFn := association.NewManyToManyBatchFunc[int, *Member, *User](
		getMembersByGroupID,
		getUsersByUserID,
		func(v *Member) int {
			return v.GroupID
		},
		func(v *Member) int {
			return v.UserID
		},
		func(v *User) int {
			return v.ID
		},
		func(_ *Member, u *User) *User {
			return u
		},
		association.WithSort(func(members []*Member) {
			sort.SliceStable(members, func(i, j int) bool {
				if members[i].Order == members[j].Order {
					return members[i].UserID < members[j].UserID
				}

				return members[i].Order < members[j].Order
			})
		}),
	)

	testCases := []struct {
		name string
		keys []int
		want map[int][]*User
	}{
		{"group 1,2,3,10", []int{1, 2, 3}, map[int][]*User{
			1:  {userData[0], userData[1], userData[4]},
			2:  {userData[2], userData[4], userData[3], userData[0]},
			3:  {userData[4], userData[3]},
			10: {},
		}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want)
		})
	}
}

func runTestCase[K comparable, V any](
	t *testing.T,
	batchFn dataloader.BatchFunc[K, V],
	keys []K,
	want map[K]V,
) {
	t.Helper()

	loader := newLoader[K, V](batchFn)
	ctx := context.Background()
	var wg sync.WaitGroup

	wg.Add(len(keys))
	for _, key := range keys {
		key := key
		go func() {
			defer wg.Done()
			got, err := loader(ctx, key)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			expect := want[key]
			if !reflect.DeepEqual(got, expect) {
				t.Errorf("got %v, want %v", got, expect)
			}
		}()
	}
	wg.Wait()
}

func newLoader[K comparable, V any](batchFn dataloader.BatchFunc[K, V]) func(ctx context.Context, key K) (V, error) {
	cache := &dataloader.NoCache[K, V]{}
	loader := dataloader.NewBatchedLoader(batchFn,
		dataloader.WithCache[K, V](cache))

	return func(ctx context.Context, key K) (v V, err error) {
		thunk := loader.Load(ctx, key)
		result, err := thunk()
		if err != nil {
			return v, err
		}

		return result, nil
	}
}
