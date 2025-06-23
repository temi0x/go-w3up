package walker_test

import (
	"io/fs"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/storacha/go-ucanto/core/iterable"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/scans/walker"
	"github.com/stretchr/testify/require"
)

func TestWalker(t *testing.T) {
	mockFS := &mockFS{
		entries: map[string][]fs.DirEntry{
			"/": {
				&mockDirEntry{name: "dir1", isDir: true},
				&mockDirEntry{name: "file1.txt", isDir: false},
			},
			"/dir1": {
				&mockDirEntry{name: "file2.txt", isDir: false},
				&mockDirEntry{name: "subdir1", isDir: true},
			},
			"/dir1/subdir1": {
				&mockDirEntry{name: "file3.txt", isDir: false},
			},
		},
	}

	mockVisitor := &mockFSVisitor{
		visitedChildren: make(map[string][]model.FSEntry),
	}

	rootPath := "/"
	rt, err := walker.WalkDir(mockFS, rootPath, mockVisitor)
	require.NoError(t, err, "WalkDir should not return an error")
	require.Equal(t, rt.Path(), "/", "Root path should match the provided root")
	require.Len(t, mockVisitor.visitedFiles, 3, "Should visit 3 files")
	require.ElementsMatch(t, mockVisitor.visitedFiles, []string{
		"/file1.txt",
		"/dir1/file2.txt",
		"/dir1/subdir1/file3.txt",
	}, "Visited files should match expected paths")

	require.Len(t, mockVisitor.visitedDirectories, 3, "Should visit 3 directories")
	require.ElementsMatch(t, mockVisitor.visitedDirectories, []string{
		"/",
		"/dir1",
		"/dir1/subdir1",
	}, "Visited directories should match expected paths")

	require.Len(t, mockVisitor.visitedChildren, 3, "Should have children for each visited directory")
	require.Len(t, mockVisitor.visitedChildren["/"], 2, "Root directory should have 2 children")
	require.ElementsMatch(t, slices.Collect(iterable.Map(func(child model.FSEntry) string {
		return child.Path()
	}, slices.Values(mockVisitor.visitedChildren["/"]))), []string{
		"/file1.txt",
		"/dir1",
	}, "Children of root directory should match expected paths")

	require.Len(t, mockVisitor.visitedChildren["/dir1"], 2, "Directory 'dir1' should have 2 children")
	require.ElementsMatch(t, slices.Collect(iterable.Map(func(child model.FSEntry) string {
		return child.Path()
	}, slices.Values(mockVisitor.visitedChildren["/dir1"]))), []string{
		"/dir1/file2.txt",
		"/dir1/subdir1",
	}, "Children of directory 'dir1' should match expected paths")
	require.Len(t, mockVisitor.visitedChildren["/dir1/subdir1"], 1, "Directory 'subdir1' should have 1 child")
	require.ElementsMatch(t, slices.Collect(iterable.Map(func(child model.FSEntry) string {
		return child.Path()
	},
		slices.Values(mockVisitor.visitedChildren["/dir1/subdir1"]))), []string{
		"/dir1/subdir1/file3.txt",
	}, "Children of directory 'subdir1' should match expected paths")
}

type mockFS struct {
	entries map[string][]fs.DirEntry
}

func (m *mockFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return m.entries[name], nil
}

func (m *mockFS) Open(name string) (fs.File, error) {
	return nil, fs.ErrNotExist // Not used in this mock
}

func (m *mockFS) Stat(name string) (fs.FileInfo, error) {
	if entries, ok := m.entries[name]; ok && len(entries) > 0 {
		return entries[0].Info()
	}
	return nil, fs.ErrNotExist
}

type mockDirEntry struct {
	name  string
	isDir bool
}

func (m *mockDirEntry) Name() string {
	return m.name
}
func (m *mockDirEntry) IsDir() bool {
	return m.isDir
}
func (m *mockDirEntry) Type() fs.FileMode {
	return 0 // Not used in this mock
}
func (m *mockDirEntry) Info() (fs.FileInfo, error) {
	return &mockFileInfo{name: m.name, isDir: m.isDir}, nil
}

type mockFileInfo struct {
	name  string
	isDir bool
}

func (m *mockFileInfo) Name() string {
	return m.name
}
func (m *mockFileInfo) Size() int64 {
	return 0 // Not used in this mock
}
func (m *mockFileInfo) Mode() fs.FileMode {
	return 0 // Not used in this mock
}
func (m *mockFileInfo) ModTime() time.Time {
	return time.Time{} // Not used in this mock
}
func (m *mockFileInfo) IsDir() bool {
	return m.isDir
}
func (m *mockFileInfo) Sys() interface{} {
	return nil // Not used in this mock
}

type mockFSVisitor struct {
	visitedFiles       []string
	visitedDirectories []string
	visitedChildren    map[string][]model.FSEntry
}

func (v *mockFSVisitor) VisitFile(path string, dirEntry fs.DirEntry) (*model.File, error) {
	v.visitedFiles = append(v.visitedFiles, path)
	return model.NewFile(path, time.Now(), 0, 0, []byte(path), uuid.New())
}
func (v *mockFSVisitor) VisitDirectory(path string, dirEntry fs.DirEntry, children []model.FSEntry) (*model.Directory, error) {
	v.visitedDirectories = append(v.visitedDirectories, path)
	v.visitedChildren[path] = children
	return model.NewDirectory(path, time.Now(), 0, []byte(path), uuid.New())
}
