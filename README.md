# GoTree

**GoTree** is a lightweight, OSTree-inspired tool for versioned filesystem / image management — written in pure Go.

It gives you a simple git-like workflow for creating, branching, mounting (via overlayfs), modifying, committing and cleaning up filesystem trees.

### The key motivation: dramatically faster edit → commit cycles

One of the biggest frustrations with full OSTree-based workflows (especially rpm-ostree layering on Fedora Silverblue / Kinoite / Universal Blue, or flatpak OSTree-heavy usage) is how **painfully slow** the mount → change → commit → unmount loop becomes once the image grows even moderately large.

GoTree solves exactly this problem by using a **much simpler data model**:

- One writable upper layer per ref (no full-tree hardlink / checkout / recommit dance)
- Direct overlayfs mounting of the layered stack
- Commit = basically just update JSON metadata + timestamp (no heavy tree scanning or object creation)
- Unmount is standard overlayfs unmount (with optional force-kill of stuck processes)

Result: you can realistically do dozens of quick edit / test / commit cycles per minute — even on multi-GB trees — where the equivalent OSTree-based operation might take 30 seconds to many minutes per cycle.

**Not production / immutable-OS ready** — this is currently a proof-of-concept / developer experimentation tool.

## Comparison: GoTree vs OSTree / rpm-ostree

| Aspect                        | GoTree                                      | OSTree / rpm-ostree                              |
|-------------------------------|---------------------------------------------|--------------------------------------------------|
| Language                      | Go (tiny binary, no deps)                   | C + GObject + many libs                          |
| Core storage model            | One writable dir per ref + overlayfs        | Content-addressed objects + hardlinks            |
| Mount → change → commit speed | Very fast (~1–5 s commit even on large trees) | Often slow (10 s – many minutes per cycle)       |
| Typical commit cost           | Write small JSON file + flush               | Full tree scan, hardlink farm, object creation   |
| Deduplication                 | No (yet)                                    | Excellent (file & block level)                   |
| Repository format             | Plain dirs + JSON refs                      | OSTree objects + bare/repo layout                |
| Use-case sweet spot           | Rapid local experimentation / dev sandboxes | Atomic OS images, immutable systems, containers  |
| Dependencies                  | None (just Linux + overlayfs)               | glib, libsoup, libarchive, gpg, etc.             |
| Atomic / reboot-to-apply      | No                                          | Yes (deployments)                                |

## Quick Start

```bash
# Create a repo anywhere (no init needed)
mkdir -p ~/gotree-repo

# Create base image
gotree ~/gotree-repo create base

# Branch for development
gotree ~/gotree-repo create my-dev base

# Mount → edit → commit loop (very fast)
sudo mkdir -p /mnt/dev
sudo gotree ~/gotree-repo mount my-dev /mnt/dev

# Make changes (as root or with appropriate perms)
sudo bash -c 'echo "Experiment v1" > /mnt/dev/test.txt'
sudo touch /mnt/dev/.wip

# Commit in < 1 second
sudo gotree ~/gotree-repo commit my-dev "First quick experiment"

# More changes, more commits — stays fast
sudo bash -c 'echo "v2" >> /mnt/dev/test.txt'
sudo gotree ~/gotree-repo commit my-dev "Iteration 2"

# When done
sudo gotree ~/gotree-repo unmount /mnt/dev    # or --force if needed