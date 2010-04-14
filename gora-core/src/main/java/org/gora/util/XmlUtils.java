package org.gora.util;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlUtils {

  public static Node getChildByName(Node n, String name) {
    NodeList children = n.getChildNodes();
    int len = children.getLength();
    for (int i = 0; i < len; i++) {
      Node childNode = children.item(i);
      if (childNode.getNodeType() != Node.ELEMENT_NODE) {
        continue;
      }
      if (childNode.getNodeName().equals(name)) {
        return childNode;
      }
    }

    return null;
  }
  
  public static String getAttribute(Node n, String attr) {
    NamedNodeMap nnm = n.getAttributes();
    if (nnm == null) {
      return null;
    }
    Node item = nnm.getNamedItem(attr);
    if (item == null) {
      return null;
    }
    return item.getNodeValue();
  }

  public static String getAttribute(Node n, String attr, String defaultValue) {
    String result = getAttribute(n, attr);
    
    return result == null ? defaultValue : result;
  }

}
