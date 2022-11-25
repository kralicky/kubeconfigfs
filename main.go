package main

import (
	"context"
	stdlog "log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	flag "github.com/spf13/pflag"
)

type RootFS struct {
	fs.Inode
}

var _ (fs.NodeCreater) = (*RootFS)(nil)
var _ (fs.NodeUnlinker) = (*RootFS)(nil)
var _ (fs.NodeLookuper) = (*RootFS)(nil)

func (r *RootFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	log.Debug("> Lookup: ", name)
	defer func() { log.Debug("< Lookup: ", node, errno) }()
	if !strings.HasSuffix(name, ".lock") {
		c := r.GetChild(name)
		if c == nil {
			return nil, syscall.ENOENT
		}
		return c, fs.OK
	}
	if inode := r.GetChild(strings.TrimSuffix(name, ".lock")); inode != nil {
		locked := inode.Operations().(*Kubeconfig).ClientcmdLocked()
		if locked {
			return r.NewInode(ctx, &fs.Inode{}, fs.StableAttr{}), fs.OK
		} else {
			return nil, syscall.ENOENT
		}
	}
	return nil, syscall.EPERM
}

func (r *RootFS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Debug("> Create: ", name)
	defer func() { log.Debug("< Create: ", node, fh, fuseFlags, errno) }()
	if !strings.HasSuffix(name, ".lock") {
		return nil, nil, 0, syscall.EPERM
	}
	if inode := r.GetChild(strings.TrimSuffix(name, ".lock")); inode != nil {
		kc, ok := inode.Operations().(*Kubeconfig)
		if !ok {
			log.Printf("not a kubeconfig: %s", name)
			return nil, nil, 0, syscall.EPERM
		}
		if ok := kc.ClientcmdLock(); ok {
			return r.NewInode(ctx, &fs.Inode{}, fs.StableAttr{}), nil, 0, fs.OK
		} else {
			return nil, nil, 0, syscall.EEXIST
		}
	}
	return nil, nil, 0, syscall.EPERM
}

func (r *RootFS) Unlink(ctx context.Context, name string) (errno syscall.Errno) {
	log.Debug("> Unlink: ", name)
	defer func() { log.Debug("< Unlink: ", errno) }()
	if !strings.HasSuffix(name, ".lock") {
		return syscall.EPERM
	}
	if inode := r.GetChild(strings.TrimSuffix(name, ".lock")); inode != nil {
		if ok := inode.Operations().(*Kubeconfig).ClientcmdUnlock(); ok {
			return fs.OK
		} else {
			return syscall.ENOENT
		}
	}
	return syscall.EPERM
}

type Options struct {
	Dirs []string
}

func main() {
	logrus.StandardLogger().SetReportCaller(true)
	logrus.StandardLogger().SetLevel(logrus.DebugLevel)

	var opts Options
	flag.StringSliceVar(&opts.Dirs, "dirs", []string{}, "directories to watch")
	flag.Parse()

	timeout := 1 * time.Second
	log.Printf("Starting")

	root := &RootFS{}

	f := NewKubeconfig(opts)

	if info, err := os.Stat(flag.Arg(0)); os.IsNotExist(err) {
		os.MkdirAll(flag.Arg(0), 0700)
	} else if err != nil {
		log.Fatalf("failed to stat %s: %v", flag.Arg(0), err)
	} else if !info.IsDir() {
		log.Fatalf("not a directory: %s", flag.Arg(0))
	}

	server, err := fs.Mount(flag.Arg(0), root, &fs.Options{
		Logger:       stdlog.Default(),
		EntryTimeout: &timeout,
		AttrTimeout:  &timeout,
		MountOptions: fuse.MountOptions{
			Debug:         true,
			DisableXAttrs: true,
		},
		UID: uint32(os.Getuid()),
		GID: uint32(os.Getgid()),
		OnAdd: func(ctx context.Context) {
			node := root.NewPersistentInode(ctx, f, fs.StableAttr{
				Mode: syscall.S_IFREG,
				Ino:  2,
			})
			root.AddChild("kubeconfig.yaml", node, true)
		},
	})
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	log.Printf("Mounted")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		if err := server.Unmount(); err != nil {
			log.Printf("Failed to unmount: %v", err)
		}
	}()

	server.Wait()
}
