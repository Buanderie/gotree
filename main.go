package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"os/exec"
)

// Ref represents a reference to an image
type Ref struct {
	Name      string            `json:"name"`
	Parent    string            `json:"parent,omitempty"`
	LayerID   string            `json:"layer_id"`
	CreatedAt time.Time         `json:"created_at"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// GoTree manages the repository
type GoTree struct {
	repoPath string
}

// NewGoTree creates a new GoTree instance
func NewGoTree(repoPath string) (*GoTree, error) {
	gt := &GoTree{repoPath: repoPath}

	// Initialize repository structure
	dirs := []string{
		filepath.Join(repoPath, "refs"),
		filepath.Join(repoPath, "layers"),
		filepath.Join(repoPath, "work"),
		filepath.Join(repoPath, "mounts"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return gt, nil
}

// ListRefs lists all available refs/images
func (gt *GoTree) ListRefs() ([]Ref, error) {
	refsDir := filepath.Join(gt.repoPath, "refs")
	entries, err := os.ReadDir(refsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read refs directory: %w", err)
	}

	var refs []Ref
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(refsDir, entry.Name()))
		if err != nil {
			continue
		}

		var ref Ref
		if err := json.Unmarshal(data, &ref); err != nil {
			continue
		}
		refs = append(refs, ref)
	}

	return refs, nil
}

// CreateEmptyRef creates a new empty ref/image
func (gt *GoTree) CreateEmptyRef(name string) error {
	if err := gt.validateRefName(name); err != nil {
		return err
	}

	layerID := gt.generateLayerID()
	layerPath := filepath.Join(gt.repoPath, "layers", layerID)

	if err := os.MkdirAll(layerPath, 0755); err != nil {
		return fmt.Errorf("failed to create layer: %w", err)
	}

	ref := Ref{
		Name:      name,
		LayerID:   layerID,
		CreatedAt: time.Now(),
		Metadata:  make(map[string]string),
	}

	return gt.saveRef(ref)
}

// CreateRefFromParent creates a new ref/image from a parent ref
func (gt *GoTree) CreateRefFromParent(name, parent string) error {
	if err := gt.validateRefName(name); err != nil {
		return err
	}

	parentRef, err := gt.getRef(parent)
	if err != nil {
		return fmt.Errorf("parent ref not found: %w", err)
	}

	layerID := gt.generateLayerID()
	layerPath := filepath.Join(gt.repoPath, "layers", layerID)

	if err := os.MkdirAll(layerPath, 0755); err != nil {
		return fmt.Errorf("failed to create layer: %w", err)
	}

	// Copy parent metadata
	metadata := make(map[string]string)
	for k, v := range parentRef.Metadata {
		metadata[k] = v
	}

	ref := Ref{
		Name:      name,
		Parent:    parent,
		LayerID:   layerID,
		CreatedAt: time.Now(),
		Metadata:  metadata,
	}

	return gt.saveRef(ref)
}

// Mount mounts a ref to a folder for read/write access
func (gt *GoTree) Mount(refName, mountPoint string) error {
	ref, err := gt.getRef(refName)
	if err != nil {
		return fmt.Errorf("ref not found: %w", err)
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	// Check if already mounted
	if gt.isMounted(mountPoint) {
		return fmt.Errorf("mount point already in use")
	}

	// Build overlay layers
	lowerDirs := gt.buildLowerDirs(ref)
	upperDir := filepath.Join(gt.repoPath, "layers", ref.LayerID)
	workDir := filepath.Join(gt.repoPath, "work", ref.LayerID)

	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("failed to create work directory: %w", err)
	}

	// Mount overlayfs
	var opts string
	if len(lowerDirs) > 0 {
		opts = fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s",
			strings.Join(lowerDirs, ":"), upperDir, workDir)
	} else {
		opts = fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s",
			upperDir, upperDir, workDir)
	}

	if err := syscall.Mount("overlay", mountPoint, "overlay", 0, opts); err != nil {
		// Fallback: use bind mount for simple case
		return syscall.Mount(upperDir, mountPoint, "", syscall.MS_BIND, "")
	}

	// Save mount info
	mountInfo := map[string]string{
		"ref":        refName,
		"mountPoint": mountPoint,
	}

	data, _ := json.Marshal(mountInfo)
	mountFile := filepath.Join(gt.repoPath, "mounts", filepath.Base(mountPoint)+".json")
	return os.WriteFile(mountFile, data, 0644)
}

// Unmount unmounts a ref from a folder
func (gt *GoTree) Unmount(mountPoint string) error {
	return gt.unmountWithOptions(mountPoint, false)
}

// UnmountForce forcibly unmounts a ref, killing processes if needed
func (gt *GoTree) UnmountForce(mountPoint string) error {
	return gt.unmountWithOptions(mountPoint, true)
}

func (gt *GoTree) unmountWithOptions(mountPoint string, force bool) error {
	if !gt.isMounted(mountPoint) {
		return fmt.Errorf("mount point not mounted")
	}
	
	absPath, err := filepath.Abs(mountPoint)
	if err != nil {
		absPath = mountPoint
	}
	
	// Sync filesystem to ensure all writes are flushed
	syscall.Sync()
	
	// Try unmount with retry logic
	var lastErr error
	maxRetries := 3
	if force {
		maxRetries = 5
	}
	
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
			syscall.Sync()
		}
		
		// On force mode and after first failure, try to kill processes
		if force && i > 0 {
			gt.killProcessesUsingMount(absPath)
			time.Sleep(200 * time.Millisecond)
		}
		
		err := syscall.Unmount(mountPoint, 0)
		if err == nil {
			// Remove mount info
			mountFile := filepath.Join(gt.repoPath, "mounts", filepath.Base(mountPoint)+".json")
			os.Remove(mountFile)
			return nil
		}
		
		lastErr = err
		
		// If still busy, try lazy unmount on last attempt
		if i == maxRetries-1 {
			err = syscall.Unmount(mountPoint, syscall.MNT_DETACH)
			if err == nil {
				mountFile := filepath.Join(gt.repoPath, "mounts", filepath.Base(mountPoint)+".json")
				os.Remove(mountFile)
				return nil
			}
		}
	}
	
	return fmt.Errorf("failed to unmount after retries: %w", lastErr)
}

// killProcessesUsingMount attempts to kill processes using the mount point
func (gt *GoTree) killProcessesUsingMount(mountPoint string) {
	// Try fuser first (more reliable)
	cmd := exec.Command("fuser", "-km", mountPoint)
	cmd.Run() // Ignore errors
	
	// Fallback: try lsof
	cmd = exec.Command("lsof", "-t", "+D", mountPoint)
	output, err := cmd.Output()
	if err == nil && len(output) > 0 {
		pids := strings.Fields(string(output))
		for _, pid := range pids {
			// Send TERM first, then KILL
			exec.Command("kill", "-TERM", pid).Run()
		}
		time.Sleep(100 * time.Millisecond)
		for _, pid := range pids {
			exec.Command("kill", "-KILL", pid).Run()
		}
	}
}

// Commit "pushes" changes from a mounted ref back to the image
func (gt *GoTree) Commit(refName, message string) error {
	ref, err := gt.getRef(refName)
	if err != nil {
		return fmt.Errorf("ref not found: %w", err)
	}

	// Update timestamp and commit message metadata
	ref.CreatedAt = time.Now()
	if ref.Metadata == nil {
		ref.Metadata = make(map[string]string)
	}
	if message != "" {
		ref.Metadata["commit.message"] = message
	}

	return gt.saveRef(*ref)
}

// SetMetadata sets a metadata key-value pair for a ref
func (gt *GoTree) SetMetadata(refName, key, value string) error {
	ref, err := gt.getRef(refName)
	if err != nil {
		return fmt.Errorf("ref not found: %w", err)
	}

	if ref.Metadata == nil {
		ref.Metadata = make(map[string]string)
	}

	ref.Metadata[key] = value
	return gt.saveRef(*ref)
}

// GetMetadata gets a metadata value for a ref
func (gt *GoTree) GetMetadata(refName, key string) (string, error) {
	ref, err := gt.getRef(refName)
	if err != nil {
		return "", fmt.Errorf("ref not found: %w", err)
	}

	if ref.Metadata == nil {
		return "", fmt.Errorf("metadata key not found: %s", key)
	}

	value, ok := ref.Metadata[key]
	if !ok {
		return "", fmt.Errorf("metadata key not found: %s", key)
	}

	return value, nil
}

// ListMetadata lists all metadata for a ref
func (gt *GoTree) ListMetadata(refName string) (map[string]string, error) {
	ref, err := gt.getRef(refName)
	if err != nil {
		return nil, fmt.Errorf("ref not found: %w", err)
	}

	if ref.Metadata == nil {
		return make(map[string]string), nil
	}

	return ref.Metadata, nil
}

// DeleteMetadata deletes a metadata key from a ref
func (gt *GoTree) DeleteMetadata(refName, key string) error {
	ref, err := gt.getRef(refName)
	if err != nil {
		return fmt.Errorf("ref not found: %w", err)
	}

	if ref.Metadata == nil {
		return nil
	}

	delete(ref.Metadata, key)
	return gt.saveRef(*ref)
}

// HasChildren returns true if any ref has this one as parent
func (gt *GoTree) HasChildren(refName string) (bool, error) {
	refs, err := gt.ListRefs()
	if err != nil {
		return false, err
	}
	for _, r := range refs {
		if r.Parent == refName {
			return true, nil
		}
	}
	return false, nil
}

// IsMountedRef checks if the ref is currently mounted anywhere
func (gt *GoTree) IsMountedRef(refName string) (bool, error) {
	mountsDir := filepath.Join(gt.repoPath, "mounts")
	entries, err := os.ReadDir(mountsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(mountsDir, entry.Name()))
		if err != nil {
			continue
		}
		var info map[string]string
		if json.Unmarshal(data, &info) != nil {
			continue
		}
		if info["ref"] == refName {
			return true, nil
		}
	}
	return false, nil
}

// DeleteRef removes a ref and its layer directory (with safety checks)
func (gt *GoTree) DeleteRef(name string, force bool) error {
	ref, err := gt.getRef(name)
	if err != nil {
		return fmt.Errorf("ref not found: %w", err)
	}

	if !force {
		hasChildren, err := gt.HasChildren(name)
		if err != nil {
			return err
		}
		if hasChildren {
			return fmt.Errorf("cannot delete ref '%s': it has child refs", name)
		}

		isMounted, err := gt.IsMountedRef(name)
		if err != nil {
			return err
		}
		if isMounted {
			return fmt.Errorf("cannot delete ref '%s': it is currently mounted", name)
		}
	}

	// Delete ref metadata file
	refPath := filepath.Join(gt.repoPath, "refs", name+".json")
	if err := os.Remove(refPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove ref file: %w", err)
	}

	// Delete layer directory
	layerPath := filepath.Join(gt.repoPath, "layers", ref.LayerID)
	if err := os.RemoveAll(layerPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove layer directory: %w", err)
	}

	// Clean up work dir (best effort)
	workPath := filepath.Join(gt.repoPath, "work", ref.LayerID)
	_ = os.RemoveAll(workPath)

	return nil
}

// Helper methods

func (gt *GoTree) validateRefName(name string) error {
	if name == "" {
		return fmt.Errorf("ref name cannot be empty")
	}
	if strings.ContainsAny(name, "/\\:*?\"<>|") {
		return fmt.Errorf("ref name contains invalid characters")
	}
	return nil
}

func (gt *GoTree) generateLayerID() string {
	return fmt.Sprintf("layer_%d", time.Now().UnixNano())
}

func (gt *GoTree) saveRef(ref Ref) error {
	data, err := json.MarshalIndent(ref, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal ref: %w", err)
	}

	refPath := filepath.Join(gt.repoPath, "refs", ref.Name+".json")
	return os.WriteFile(refPath, data, 0644)
}

func (gt *GoTree) getRef(name string) (*Ref, error) {
	refPath := filepath.Join(gt.repoPath, "refs", name+".json")
	data, err := os.ReadFile(refPath)
	if err != nil {
		return nil, err
	}

	var ref Ref
	if err := json.Unmarshal(data, &ref); err != nil {
		return nil, err
	}

	return &ref, nil
}

func (gt *GoTree) buildLowerDirs(ref *Ref) []string {
	var dirs []string

	current := ref
	for current.Parent != "" {
		parent, err := gt.getRef(current.Parent)
		if err != nil {
			break
		}
		dirs = append(dirs, filepath.Join(gt.repoPath, "layers", parent.LayerID))
		current = parent
	}

	return dirs
}

func (gt *GoTree) isMounted(mountPoint string) bool {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false
	}

	absPath, _ := filepath.Abs(mountPoint)
	return strings.Contains(string(data), absPath)
}

// dirSize returns the apparent size (sum of file sizes) of all regular files in the directory tree
func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil // skip symlinks
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// formatBytes converts bytes to human readable format (KiB, MiB, GiB, etc.)
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// CLI interface

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}

	repoPath := os.Args[1]
	command := os.Args[2]

	gt, err := NewGoTree(repoPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing GoTree: %v\n", err)
		os.Exit(1)
	}

	switch command {
	case "list":
		refs, err := gt.ListRefs()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listing refs: %v\n", err)
			os.Exit(1)
		}
		for _, ref := range refs {
			parent := ""
			if ref.Parent != "" {
				parent = fmt.Sprintf(" (parent: %s)", ref.Parent)
			}
			metadata := ""
			if len(ref.Metadata) > 0 {
				metadata = " ["
				first := true
				for k, v := range ref.Metadata {
					if !first {
						metadata += ", "
					}
					metadata += fmt.Sprintf("%s=%s", k, v)
					first = false
				}
				metadata += "]"
			}
			fmt.Printf("%s%s%s - %s\n", ref.Name, parent, metadata, ref.CreatedAt.Format(time.RFC3339))
		}

	case "create":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "Usage: %s <repo> create <name> [parent]\n", os.Args[0])
			os.Exit(1)
		}
		name := os.Args[3]

		var err error
		if len(os.Args) == 5 {
			parent := os.Args[4]
			err = gt.CreateRefFromParent(name, parent)
		} else {
			err = gt.CreateEmptyRef(name)
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating ref: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Created ref: %s\n", name)

	case "mount":
		if len(os.Args) < 5 {
			fmt.Fprintf(os.Stderr, "Usage: %s <repo> mount <ref> <mountpoint>\n", os.Args[0])
			os.Exit(1)
		}
		refName := os.Args[3]
		mountPoint := os.Args[4]

		if err := gt.Mount(refName, mountPoint); err != nil {
			fmt.Fprintf(os.Stderr, "Error mounting: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Mounted %s to %s\n", refName, mountPoint)

	case "unmount":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "Usage: %s <repo> unmount <mountpoint> [--force]\n", os.Args[0])
			os.Exit(1)
		}
		mountPoint := os.Args[3]
		force := false
		if len(os.Args) > 4 && os.Args[4] == "--force" {
			force = true
		}
		
		var err error
		if force {
			err = gt.UnmountForce(mountPoint)
		} else {
			err = gt.Unmount(mountPoint)
		}
		
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error unmounting: %v\n", err)
			if !force {
				fmt.Fprintf(os.Stderr, "Hint: Try with --force flag to kill processes using the mount\n")
			}
			os.Exit(1)
		}
		fmt.Printf("Unmounted %s\n", mountPoint)

	case "commit":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "Usage: %s <repo> commit <ref> [message]\n", os.Args[0])
			os.Exit(1)
		}
		refName := os.Args[3]
		message := ""
		if len(os.Args) > 4 {
			message = os.Args[4]
		}

		if err := gt.Commit(refName, message); err != nil {
			fmt.Fprintf(os.Stderr, "Error committing: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Committed changes to %s\n", refName)

	case "size":
		if len(os.Args) < 4 {
			os.Exit(1)
		}
		refName := os.Args[3]

		ref, err := gt.getRef(refName)
		if err != nil {
			os.Exit(1)
		}

		var totalSize int64
		current := ref
		seen := make(map[string]bool) // basic cycle protection

		for {
			if seen[current.LayerID] {
				break
			}
			seen[current.LayerID] = true

			layerPath := filepath.Join(gt.repoPath, "layers", current.LayerID)
			s, err := dirSize(layerPath)
			if err == nil {
				totalSize += s
			}

			if current.Parent == "" {
				break
			}

			parentRef, err := gt.getRef(current.Parent)
			if err != nil {
				break
			}
			current = parentRef
		}

		fmt.Printf("%d\n", totalSize)

	case "delete", "rm":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "Usage: %s <repo> delete <ref> [--force]\n", os.Args[0])
			fmt.Fprintf(os.Stderr, "   or: %s <repo> rm <ref> [--force]\n", os.Args[0])
			os.Exit(1)
		}
		refName := os.Args[3]
		force := len(os.Args) >= 5 && os.Args[4] == "--force"

		if err := gt.DeleteRef(refName, force); err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting ref: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Deleted ref: %s\n", refName)

	case "metadata":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "Usage: %s <repo> metadata <subcommand> ...\n", os.Args[0])
			fmt.Fprintf(os.Stderr, "Subcommands:\n")
			fmt.Fprintf(os.Stderr, "  set <ref> <key> <value>  - Set metadata\n")
			fmt.Fprintf(os.Stderr, "  get <ref> <key>          - Get metadata\n")
			fmt.Fprintf(os.Stderr, "  list <ref>               - List all metadata\n")
			fmt.Fprintf(os.Stderr, "  delete <ref> <key>       - Delete metadata\n")
			os.Exit(1)
		}

		subcommand := os.Args[3]

		switch subcommand {
		case "set":
			if len(os.Args) < 7 {
				fmt.Fprintf(os.Stderr, "Usage: %s <repo> metadata set <ref> <key> <value>\n", os.Args[0])
				os.Exit(1)
			}
			refName := os.Args[4]
			key := os.Args[5]
			value := os.Args[6]

			if err := gt.SetMetadata(refName, key, value); err != nil {
				fmt.Fprintf(os.Stderr, "Error setting metadata: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Set %s=%s on ref %s\n", key, value, refName)

		case "get":
			if len(os.Args) < 6 {
				fmt.Fprintf(os.Stderr, "Usage: %s <repo> metadata get <ref> <key>\n", os.Args[0])
				os.Exit(1)
			}
			refName := os.Args[4]
			key := os.Args[5]

			value, err := gt.GetMetadata(refName, key)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error getting metadata: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("%s\n", value)

		case "list":
			if len(os.Args) < 5 {
				fmt.Fprintf(os.Stderr, "Usage: %s <repo> metadata list <ref>\n", os.Args[0])
				os.Exit(1)
			}
			refName := os.Args[4]

			metadata, err := gt.ListMetadata(refName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error listing metadata: %v\n", err)
				os.Exit(1)
			}

			if len(metadata) == 0 {
				fmt.Println("No metadata")
			} else {
				for k, v := range metadata {
					fmt.Printf("%s=%s\n", k, v)
				}
			}

		case "delete":
			if len(os.Args) < 6 {
				fmt.Fprintf(os.Stderr, "Usage: %s <repo> metadata delete <ref> <key>\n", os.Args[0])
				os.Exit(1)
			}
			refName := os.Args[4]
			key := os.Args[5]

			if err := gt.DeleteMetadata(refName, key); err != nil {
				fmt.Fprintf(os.Stderr, "Error deleting metadata: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Deleted metadata key %s from ref %s\n", key, refName)

		default:
			fmt.Fprintf(os.Stderr, "Unknown metadata subcommand: %s\n", subcommand)
			os.Exit(1)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("GoTree - OSTree-like system in Go")
	fmt.Println("\nUsage:")
	fmt.Println("  gotree <repo> list")
	fmt.Println("  gotree <repo> create <name> [parent]")
	fmt.Println("  gotree <repo> mount <ref> <mountpoint>")
	fmt.Println("  gotree <repo> unmount <mountpoint>")
	fmt.Println("  gotree <repo> commit <ref> [message]")
	fmt.Println("  gotree <repo> size <ref>")
	fmt.Println("  gotree <repo> delete <ref> [--force]")
	fmt.Println("  gotree <repo> rm <ref> [--force]          (alias)")
	fmt.Println("\nExamples:")
	fmt.Println("  gotree /var/lib/gotree list")
	fmt.Println("  gotree /var/lib/gotree create base")
	fmt.Println("  gotree /var/lib/gotree create dev base")
	fmt.Println("  gotree /var/lib/gotree mount dev /mnt/dev")
	fmt.Println("  gotree /var/lib/gotree unmount /mnt/dev")
	fmt.Println("  gotree /var/lib/gotree commit dev 'Added new files'")
	fmt.Println("  gotree /var/lib/gotree size dev")
	fmt.Println("  gotree /var/lib/gotree delete old-experiment")
	fmt.Println("  gotree /var/lib/gotree rm base --force")
}