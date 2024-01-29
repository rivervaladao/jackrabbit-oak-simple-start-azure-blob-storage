package org.example;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

public class NodePrinter {

	public static void print(Node node) throws RepositoryException {
		System.out.println("Printing content of node: " + node.getPath());
		System.out.println(" Properties:");

		for (PropertyIterator pi = node.getProperties(); pi.hasNext();) {
			Property p = pi.nextProperty();
			if (p.getDefinition().isMultiple()) {
				Value[] values = p.getValues();
				for (int i = 0; i < values.length; i++) {
					System.out.println("  - " + p.getName() + ": " + values[i].getString());
				}
			} else {
				if(p.getType() != PropertyType.BINARY)
				System.out.println("  - " + p.getName() + ": " + p.getValue().getString());
			}
		}
		System.out.println(" Child nodes:");
		for (NodeIterator ni = node.getNodes(); ni.hasNext();) {
			Node n = ni.nextNode();
			System.out.println("  - " + n.getName());
			print(n);
		}
	}
}