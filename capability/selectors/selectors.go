package selectors

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

// SelectByPath creates a selector that retrieves nodes by their path.
func SelectByPath(path string) (selector.Selector, error) {
	// Create a new selector that matches the specified path
	sel, err := selector.Compile(path)
	if err != nil {
		return nil, err
	}
	return sel, nil
}

// TraverseDAG traverses the DAG using the provided selector.
func TraverseDAG(node ipld.Node, sel selector.Selector) ([]ipld.Node, error) {
	var results []ipld.Node

	// Use the selector to traverse the node
	err := selector.Select(node, sel, func(n ipld.Node) error {
		results = append(results, n)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}