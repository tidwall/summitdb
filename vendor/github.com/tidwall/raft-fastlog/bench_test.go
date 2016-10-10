package raftfastlog

import (
	"os"
	"testing"

	"github.com/hashicorp/raft/bench"
)

func BenchmarkFastLogStore_FirstIndex(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkFastLogStore_LastIndex(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.LastIndex(b, store)
}

func BenchmarkFastLogStore_GetLog(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetLog(b, store)
}

func BenchmarkFastLogStore_StoreLog(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLog(b, store)
}

func BenchmarkFastLogStore_StoreLogs(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkFastLogStore_DeleteRange(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkFastLogStore_Set(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Set(b, store)
}

func BenchmarkFastLogStore_Get(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Get(b, store)
}

func BenchmarkFastLogStore_SetUint64(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.SetUint64(b, store)
}

func BenchmarkFastLogStore_GetUint64(b *testing.B) {
	store := testFastLogStore(b, false)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetUint64(b, store)
}

func BenchmarkFastLogStore_FirstIndex_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkFastLogStore_LastIndex_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.LastIndex(b, store)
}

func BenchmarkFastLogStore_GetLog_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetLog(b, store)
}

func BenchmarkFastLogStore_StoreLog_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLog(b, store)
}

func BenchmarkFastLogStore_StoreLogs_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkFastLogStore_DeleteRange_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkFastLogStore_Set_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Set(b, store)
}

func BenchmarkFastLogStore_Get_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Get(b, store)
}

func BenchmarkFastLogStore_SetUint64_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.SetUint64(b, store)
}

func BenchmarkFastLogStore_GetUint64_InMem(b *testing.B) {
	store := testFastLogStore(b, true)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetUint64(b, store)
}
