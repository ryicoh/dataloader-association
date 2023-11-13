package association_test

import (
	"context"
	"errors"
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
		{UserID: 1, GroupID: 1},
		{UserID: 1, GroupID: 2},
		{UserID: 2, GroupID: 1},
		{UserID: 3, GroupID: 2},
		{UserID: 4, GroupID: 2},
		{UserID: 4, GroupID: 3},
		{UserID: 5, GroupID: 1},
		{UserID: 5, GroupID: 2},
		{UserID: 5, GroupID: 3},
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
		errs map[int]error
	}{
		{"empty", []int{}, map[int]*User{}, nil},
		{"user 1", []int{1}, map[int]*User{1: userData[0]}, nil},
		{"user 1,3", []int{1, 3}, map[int]*User{1: userData[0], 3: userData[2]}, nil},
		{"user 2", []int{2}, map[int]*User{2: userData[1]}, nil},
		{"user 10", []int{10}, map[int]*User{}, map[int]error{10: association.ErrNotFound}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want, tc.errs)
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
		errs map[int]error
	}{
		{"empty", []int{}, map[int][]*User{}, nil},
		{"group 1", []int{1}, map[int][]*User{1: {userData[0], userData[1]}}, nil},
		{"group 1,3", []int{1, 3}, map[int][]*User{1: {userData[0], userData[1]}, 3: {userData[4]}}, nil},
		{"group 1,2,3", []int{1, 2, 3}, map[int][]*User{1: {userData[0], userData[1]}, 2: {userData[2], userData[3]}, 3: {userData[4]}}, nil},
		{"group 1,2,3,10", []int{1, 2, 3}, map[int][]*User{1: {userData[0], userData[1]}, 2: {userData[2], userData[3]}, 3: {userData[4]}}, map[int]error{10: association.ErrNotFound}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want, tc.errs)
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
	)

	testCases := []struct {
		name string
		keys []int
		want map[int][]*User
		errs map[int]error
	}{
		{"empty", []int{}, map[int][]*User{}, nil},
		{"group 1", []int{1}, map[int][]*User{1: {userData[0], userData[1], userData[4]}}, nil},
		{"group 1,3", []int{1, 3}, map[int][]*User{1: {userData[0], userData[1], userData[4]}, 3: {userData[3], userData[4]}}, nil},
		{"group 1,2,3", []int{1, 2, 3}, map[int][]*User{1: {userData[0], userData[1], userData[4]}, 2: {userData[0], userData[2], userData[3], userData[4]}, 3: {userData[3], userData[4]}}, nil},
		{"group 1,2,3,10", []int{1, 2, 3}, map[int][]*User{1: {userData[0], userData[1], userData[4]}, 2: {userData[0], userData[2], userData[3], userData[4]}, 3: {userData[3], userData[4]}}, map[int]error{10: association.ErrNotFound}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want, nil)
		})
	}
}

func TestNewManyToManyBatchFuncWithSortFunc(t *testing.T) {
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
		association.WithSortFunc(func(users []*User) {
			sort.SliceStable(users, func(i, j int) bool {
				return users[i].ID > users[j].ID
			})
		}),
	)

	testCases := []struct {
		name string
		keys []int
		want map[int][]*User
		errs map[int]error
	}{
		{"group 1,2,3,10", []int{1, 2, 3}, map[int][]*User{1: {userData[4], userData[1], userData[0]}, 2: {userData[4], userData[3], userData[2], userData[0]}, 3: {userData[4], userData[3]}}, map[int]error{10: association.ErrNotFound}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, batchFn, tc.keys, tc.want, nil)
		})
	}
}

func runTestCase[K comparable, V any](
	t *testing.T,
	batchFn dataloader.BatchFunc[K, V],
	keys []K,
	want map[K]V,
	errs map[K]error,
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
			if !errors.Is(err, errs[key]) {
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
