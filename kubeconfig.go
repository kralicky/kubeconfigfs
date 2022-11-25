package main

import (
	"bufio"
	"bytes"
	"context"
	iofs "io/fs"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/samber/lo"
	"k8s.io/client-go/tools/clientcmd"
)

type CurrentContext struct {
	mu          sync.Mutex
	value       string
	lastUpdated time.Time
}

type SharedAttr struct {
	fuse.Attr
	sync.Mutex
}

func (c *CurrentContext) Get() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastUpdated.IsZero() {
		return "", false
	}
	return c.value, true
}

func (c *CurrentContext) Mtime() (uint64, uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// return seconds and nanosecond components of the time
	t := c.lastUpdated
	return uint64(t.Unix()), uint32(t.Nanosecond())
}

func (c *CurrentContext) Observe(value string, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastUpdated.IsZero() && c.value == "" && value != "" {
		c.value = value
		c.lastUpdated = t
	}
}

func (c *CurrentContext) Set(value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = value
	c.lastUpdated = time.Now()
}

type Kubeconfig struct {
	fs.Inode
	Options

	mu              sync.Mutex
	clientcmdLocked bool
	Attr            SharedAttr
	currentContext  CurrentContext
}

func (kc *Kubeconfig) ClientcmdLock() bool {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	if kc.clientcmdLocked {
		return false
	}
	kc.clientcmdLocked = true
	return true
}

func (kc *Kubeconfig) ClientcmdUnlock() bool {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	if !kc.clientcmdLocked {
		return false
	}
	kc.clientcmdLocked = false
	return true
}

func (kc *Kubeconfig) ClientcmdLocked() bool {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	return kc.clientcmdLocked
}

func NewKubeconfig(opts Options) *Kubeconfig {
	attr := fuse.Attr{
		Mode:   0600,
		Size:   0,
		Blocks: 0,
		Nlink:  1,
		Owner:  *fuse.CurrentOwner(),
	}
	now := time.Now()
	attr.SetTimes(&now, &now, &now)
	kc := &Kubeconfig{
		Options: opts,
		Attr: SharedAttr{
			Attr: attr,
		},
	}
	return kc
}

var _ = (fs.NodeOpener)((*Kubeconfig)(nil))
var _ = (fs.NodeLookuper)((*Kubeconfig)(nil))
var _ = (fs.NodeGetattrer)((*Kubeconfig)(nil))
var _ = (fs.NodeSetattrer)((*Kubeconfig)(nil))

func (f *Kubeconfig) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return f.newAllKubeconfigs(&f.currentContext), fuse.FOPEN_DIRECT_IO, 0
}

func (r *Kubeconfig) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (node *fs.Inode, errno syscall.Errno) {
	log.Println("> Lookup", name)
	return r.EmbeddedInode(), fs.OK
}

func (f *Kubeconfig) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Println("> Getattr")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Attr.Lock()
	defer f.Attr.Unlock()
	out.Attr = f.Attr.Attr
	out.SetTimeout(1 * time.Second)
	return fs.OK
}

func (f *Kubeconfig) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	log.Println("> Setattr")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Attr.Lock()
	defer f.Attr.Unlock()
	out.Attr = f.Attr.Attr
	out.SetTimeout(1 * time.Second)
	return fs.OK
}

type AllKubeconfigsHandle struct {
	currentContext *CurrentContext
	attr           *SharedAttr
	content        []byte
	err            syscall.Errno
	wait           chan struct{}
}

var _ = (fs.FileReader)((*AllKubeconfigsHandle)(nil))
var _ = (fs.FileWriter)((*AllKubeconfigsHandle)(nil))
var _ = (fs.FileGetattrer)((*AllKubeconfigsHandle)(nil))

func (fh *AllKubeconfigsHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	select {
	case <-ctx.Done():
		return nil, syscall.EINTR
	case <-fh.wait:
	}
	if fh.err != 0 {
		return nil, fh.err
	}
	end := off + int64(len(dest))
	if end > int64(len(fh.content)) {
		end = int64(len(fh.content))
	}
	now := time.Now()
	fh.attr.Lock()
	mod, ch := fh.attr.ModTime(), fh.attr.ChangeTime()
	fh.attr.SetTimes(&now, &mod, &ch)
	fh.attr.Unlock()
	return fuse.ReadResultData(fh.content[off:end]), 0
}

func (fh *AllKubeconfigsHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	// Scan lines for 'current-context: name' and update the current context
	// if it's different from the current one.
	for scanner := bufio.NewScanner(bytes.NewReader(data)); scanner.Scan(); {
		line := scanner.Text()
		if strings.HasPrefix(line, "current-context: ") {
			name := strings.TrimPrefix(line, "current-context: ")
			fh.currentContext.Set(name)
			now := time.Now()
			fh.attr.Lock()
			ch := fh.attr.ChangeTime()
			fh.attr.SetTimes(&now, &now, &ch)
			fh.attr.Unlock()
			log.Println("> Updated current context to", name)
			break
		}
	}
	return uint32(len(data)), fs.OK
}

func (fh *AllKubeconfigsHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	fh.attr.Lock()
	defer fh.attr.Unlock()
	fh.attr.Lock()
	defer fh.attr.Unlock()
	out.Attr = fh.attr.Attr
	out.SetTimeout(1 * time.Second)
	return fs.OK
}

func (f *Kubeconfig) newAllKubeconfigs(cc *CurrentContext) *AllKubeconfigsHandle {
	fh := &AllKubeconfigsHandle{
		currentContext: cc,
		attr:           &f.Attr,
		wait:           make(chan struct{}),
	}
	go func() {
		defer close(fh.wait)
		kubeconfigs := []string{}
		for _, dir := range f.Dirs {
			filepath.Walk(dir, func(path string, info iofs.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}

				ext := filepath.Ext(path)
				if ext == ".yaml" || ext == ".yml" || ext == ".json" {
					kubeconfigs = append(kubeconfigs, path)
				}
				return nil
			})
		}
		log.Println("Found", len(kubeconfigs), "kubeconfigs")

		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		rules.Precedence = lo.Uniq(append(rules.Precedence, kubeconfigs...))

		mergedConfig, err := rules.Load()
		if err != nil {
			log.Println(err)
			fh.err = syscall.EIO
			return
		}

		cc.Observe(mergedConfig.CurrentContext, time.Now())
		if c, ok := cc.Get(); ok {
			log.Println("Using current context:", c)
			mergedConfig.CurrentContext = c
		}

		mergedBytes, err := clientcmd.Write(*mergedConfig)
		if err != nil {
			log.Println(err)
			fh.err = syscall.EIO
			return
		}

		fh.content = mergedBytes
	}()
	return fh
}
