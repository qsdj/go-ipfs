package commands

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	cmdenv "github.com/ipfs/go-ipfs/core/commands/cmdenv"
	e "github.com/ipfs/go-ipfs/core/commands/e"
	iface "github.com/ipfs/go-ipfs/core/coreapi/interface"

	offline "gx/ipfs/QmPpnbwgAuvhUkA9jGooR88ZwZtTUHXXvoQNKdjZC6nYku/go-ipfs-exchange-offline"
	unixfs "gx/ipfs/QmQFXpLVAgoQsgsdJspTkHcVpkLznHSTovwaBcBPvvLNoG/go-unixfs"
	uio "gx/ipfs/QmQFXpLVAgoQsgsdJspTkHcVpkLznHSTovwaBcBPvvLNoG/go-unixfs/io"
	unixfs_pb "gx/ipfs/QmQFXpLVAgoQsgsdJspTkHcVpkLznHSTovwaBcBPvvLNoG/go-unixfs/pb"
	cid "gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	blockservice "gx/ipfs/QmVPeMNK9DfGLXDZzs2W4RoFWC9Zq1EnLGmLXtYtWrNdcW/go-blockservice"
	cmds "gx/ipfs/Qma6uuSyjkecGhMFFLfzyJDPyoDtNJSHJNweDccZhaWkgU/go-ipfs-cmds"
	merkledag "gx/ipfs/QmcBudYCTxMfZ63BoPD5DNPksxaxtS6zQrdJvVSxKqktin/go-merkledag"
	ipld "gx/ipfs/QmcKKBwfz6FyQdHR2jsXrrF6XeSBXYL86anmWNewpFpoF5/go-ipld-format"
	"gx/ipfs/Qmde5VP1qUkyQXKCfmEUA7bP64V2HAptbJ7phuPp7jXWwg/go-ipfs-cmdkit"
)

// LsLink contains printable data for a single ipld link in ls output
type LsLink struct {
	Name, Hash string
	Size       uint64
	Type       unixfs_pb.Data_DataType
}

// LsObject is an element of LsOutput
// It can represent all or part of a directory
type LsObject struct {
	Hash  string
	Links []LsLink
}

// LsOutput is a set of printable data for directories,
// it can be complete or partial
type LsOutput struct {
	Objects []LsObject
}

const (
	lsHeadersOptionNameTime = "headers"
	lsResolveTypeOptionName = "resolve-type"
	lsStreamOptionName      = "stream"
)

var LsCmd = &cmds.Command{
	Helptext: cmdkit.HelpText{
		Tagline: "List directory contents for Unix filesystem objects.",
		ShortDescription: `
Displays the contents of an IPFS or IPNS object(s) at the given path, with
the following format:

  <link base58 hash> <link size in bytes> <link name>

The JSON output contains type information.
`,
	},

	Arguments: []cmdkit.Argument{
		cmdkit.StringArg("ipfs-path", true, true, "The path to the IPFS object(s) to list links from.").EnableStdin(),
	},
	Options: []cmdkit.Option{
		cmdkit.BoolOption(lsHeadersOptionNameTime, "v", "Print table headers (Hash, Size, Name)."),
		cmdkit.BoolOption(lsResolveTypeOptionName, "Resolve linked objects to find out their types.").WithDefault(true),
		cmdkit.BoolOption(lsStreamOptionName, "s", "Stream directory entries as they are found."),
	},
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		nd, err := cmdenv.GetNode(env)
		if err != nil {
			return err
		}

		api, err := cmdenv.GetApi(env)
		if err != nil {
			return err
		}

		resolve, _ := req.Options[lsResolveTypeOptionName].(bool)
		dserv := nd.DAG
		if !resolve {
			offlineexch := offline.Exchange(nd.Blockstore)
			bserv := blockservice.New(nd.Blockstore, offlineexch)
			dserv = merkledag.NewDAGService(bserv)
		}

		err = req.ParseBodyArgs()
		if err != nil {
			return err
		}

		paths := req.Arguments

		var dagnodes []ipld.Node
		for _, fpath := range paths {
			p, err := iface.ParsePath(fpath)
			if err != nil {
				return err
			}

			dagnode, err := api.ResolveNode(req.Context, p)
			if err != nil {
				return err
			}
			dagnodes = append(dagnodes, dagnode)
		}
		ng := merkledag.NewSession(req.Context, nd.DAG)
		ro := merkledag.NewReadOnlyDagService(ng)

		stream, _ := req.Options[lsStreamOptionName].(bool)
		if !stream {
			output := make([]LsObject, len(req.Arguments))

			for i, dagnode := range dagnodes {
				dir, err := uio.NewDirectoryFromNode(ro, dagnode)
				if err != nil && err != uio.ErrNotADir {
					return fmt.Errorf("the data in %s (at %q) is not a UnixFS directory: %s", dagnode.Cid(), paths[i], err)
				}

				var links []*ipld.Link
				if dir == nil {
					links = dagnode.Links()
				} else {
					links, err = dir.Links(req.Context)
					if err != nil {
						return err
					}
				}
				outputLinks := make([]LsLink, len(links))
				for j, link := range links {
					lsLink, err := makeLsLink(req, dserv, resolve, link)
					if err != nil {
						return err
					}
					outputLinks[j] = *lsLink
				}
				output[i] = LsObject{
					Hash:  paths[i],
					Links: outputLinks,
				}
			}

			return cmds.EmitOnce(res, &LsOutput{output})
		}

		for i, dagnode := range dagnodes {
			dir, err := uio.NewDirectoryFromNode(ro, dagnode)
			if err != nil && err != uio.ErrNotADir {
				return fmt.Errorf("the data in %s (at %q) is not a UnixFS directory: %s", dagnode.Cid(), paths[i], err)
			}

			var linkResults <-chan unixfs.LinkResult
			if dir == nil {
				linkResults = makeDagNodeLinkResults(req, dagnode)
			} else {
				linkResults = dir.EnumLinksAsync(req.Context)
			}

			for linkResult := range linkResults {
				output := make([]LsObject, len(req.Arguments))

				for i, path := range paths {
					output[i] = LsObject{
						Hash:  path,
						Links: nil,
					}
				}
				outputLinks := make([]LsLink, 1)

				if linkResult.Err != nil {
					return linkResult.Err
				}
				link := linkResult.Link
				lsLink, err := makeLsLink(req, dserv, resolve, link)
				if err != nil {
					return err
				}
				outputLinks[0] = *lsLink
				output[i].Links = outputLinks
				if err = res.Emit(&LsOutput{output}); err != nil {
					return err
				}
			}
		}
		return nil
	},
	PostRun: cmds.PostRunMap{
		cmds.CLI: func(res cmds.Response, re cmds.ResponseEmitter) error {
			req := res.Request()
			headers, _ := req.Options[lsHeadersOptionNameTime].(bool)
			stream, _ := req.Options[lsStreamOptionName].(bool)

			// in streaming mode we can't automatically align the tabs
			// so we take a best guess
			var minTabWidth int
			if stream {
				minTabWidth = 10
			} else {
				minTabWidth = 1
			}

			multipleFolders := len(req.Arguments) > 1
			lastDirectoryWritten := -1

			tw := tabwriter.NewWriter(os.Stdout, minTabWidth, 2, 1, ' ', 0)
			for {
				v, err := res.Next()
				if err != nil {
					if err == io.EOF {
						if multipleFolders {
							fmt.Fprintln(os.Stdout)
						}
						return nil
					}

					return err
				}

				output, ok := v.(*LsOutput)
				if !ok {
					return e.TypeErr(output, v)
				}

				for i, object := range output.Objects {
					if len(object.Links) == 0 {
						continue
					}
					if i > lastDirectoryWritten {
						if i > 0 {
							if multipleFolders {
								fmt.Fprintln(tw)
							}
						}
						if multipleFolders {
							fmt.Fprintf(tw, "%s:\n", object.Hash)
						}
						if headers {
							fmt.Fprintln(tw, "Hash\tSize\tName")
						}
						lastDirectoryWritten = i
					}
					for _, link := range object.Links {
						if link.Type == unixfs.TDirectory {
							link.Name += "/"
						}

						fmt.Fprintf(tw, "%s\t%v\t%s\n", link.Hash, link.Size, link.Name)
					}
				}
				tw.Flush()
			}
		},
	},
	Type: LsOutput{},
}

func makeDagNodeLinkResults(req *cmds.Request, dagnode ipld.Node) <-chan unixfs.LinkResult {
	linkResults := make(chan unixfs.LinkResult)
	go func() {
		defer close(linkResults)
		for _, l := range dagnode.Links() {
			select {
			case linkResults <- unixfs.LinkResult{
				Link: l,
				Err:  nil,
			}:
			case <-req.Context.Done():
				return
			}
		}
	}()
	return linkResults
}

func makeLsLink(req *cmds.Request, dserv ipld.DAGService, resolve bool, link *ipld.Link) (*LsLink, error) {
	t := unixfs_pb.Data_DataType(-1)

	switch link.Cid.Type() {
	case cid.Raw:
		// No need to check with raw leaves
		t = unixfs.TFile
	case cid.DagProtobuf:
		linkNode, err := link.GetNode(req.Context, dserv)
		if err == ipld.ErrNotFound && !resolve {
			// not an error
			linkNode = nil
		} else if err != nil {
			return nil, err
		}

		if pn, ok := linkNode.(*merkledag.ProtoNode); ok {
			d, err := unixfs.FSNodeFromBytes(pn.Data())
			if err != nil {
				return nil, err
			}
			t = d.Type()
		}
	}
	return &LsLink{
		Name: link.Name,
		Hash: link.Cid.String(),
		Size: link.Size,
		Type: t,
	}, nil
}
