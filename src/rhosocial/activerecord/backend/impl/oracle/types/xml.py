# types/xml.py
"""
Oracle XMLType type definition.

XMLType is Oracle's native XML data type, providing methods
for XML manipulation and querying.
"""

from dataclasses import dataclass, field
from typing import Optional, Any
from xml.etree.ElementTree import Element, fromstring, tostring, ParseError


@dataclass
class OracleXMLType:
    """Oracle XMLType type representation.
    
    XMLType is Oracle's native data type for storing and
    processing XML data. This class provides Python-side
    representation and basic manipulation.
    
    Example:
        >>> xml = OracleXMLType("<root><name>John</name></root>")
        >>> xml.is_valid
        True
        >>> xml.extract("/root/name")
        'John'
    
    Attributes:
        content: The XML content as string
        _root: Parsed root element (internal)
    """
    content: str
    _root: Optional[Element] = field(default=None, repr=False)
    
    def __post_init__(self):
        if self.content and self._root is None:
            try:
                object.__setattr__(self, '_root', fromstring(self.content))
            except ParseError:
                pass
    
    @property
    def root(self) -> Optional[Element]:
        """Get root element if valid XML.
        
        Returns:
            ElementTree Element or None if invalid
        """
        return self._root
    
    @property
    def is_valid(self) -> bool:
        """Check if content is valid XML.
        
        Returns:
            True if XML parsed successfully
        """
        return self._root is not None
    
    def extract(self, xpath: str) -> Optional[str]:
        """Extract value using XPath expression.
        
        This is a simplified implementation supporting basic
        element paths. For full XPath support, use Oracle's
        XMLType methods on the database side.
        
        Args:
            xpath: XPath expression (simplified, e.g., '/root/child')
            
        Returns:
            Extracted value or None if not found
        """
        if self._root is None:
            return None
        
        parts = xpath.strip('/').split('/')
        if not parts or parts == ['']:
            return self.content
        
        element = self._root
        for i, part in enumerate(parts):
            if not part:
                continue
            
            # Handle attribute access
            if part.startswith('@'):
                attr_name = part[1:]
                if i == len(parts) - 1:
                    return element.get(attr_name)
                return None
            
            # Handle element access
            try:
                element = element.find(part)
                if element is None:
                    return None
            except Exception:
                return None
        
        return element.text if element is not None else None
    
    def exists(self, xpath: str) -> bool:
        """Check if XPath exists in document.
        
        Args:
            xpath: XPath expression to check
            
        Returns:
            True if path exists
        """
        return self.extract(xpath) is not None
    
    def to_string(self, encoding: str = 'unicode') -> str:
        """Convert to string representation.
        
        Args:
            encoding: Output encoding ('unicode' for str)
            
        Returns:
            XML string
        """
        if self._root is not None:
            return tostring(self._root, encoding=encoding)
        return self.content
    
    def get_element(self, xpath: str) -> Optional[Element]:
        """Get Element for XPath.
        
        Args:
            xpath: XPath expression
            
        Returns:
            Element or None if not found
        """
        if self._root is None:
            return None
        
        parts = xpath.strip('/').split('/')
        element = self._root
        
        for part in parts:
            if not part:
                continue
            element = element.find(part)
            if element is None:
                return None
        
        return element
    
    def get_children(self, xpath: str = '') -> list:
        """Get child elements at XPath.
        
        Args:
            xpath: Parent XPath (empty for root children)
            
        Returns:
            List of child Elements
        """
        if xpath:
            parent = self.get_element(xpath)
        else:
            parent = self._root
        
        if parent is None:
            return []
        
        return list(parent)
    
    @classmethod
    def from_element(cls, element: Element) -> 'OracleXMLType':
        """Create from ElementTree Element.
        
        Args:
            element: ElementTree Element
            
        Returns:
            New OracleXMLType instance
        """
        content = tostring(element, encoding='unicode')
        obj = cls(content=content)
        object.__setattr__(obj, '_root', element)
        return obj
    
    @classmethod
    def from_dict(cls, d: dict, root_name: str = 'root') -> 'OracleXMLType':
        """Create from dictionary (simplified conversion).
        
        Args:
            d: Dictionary to convert
            root_name: Name for root element
            
        Returns:
            New OracleXMLType instance
        """
        def dict_to_xml(name: str, value: Any) -> Element:
            elem = Element(name)
            if isinstance(value, dict):
                for k, v in value.items():
                    elem.append(dict_to_xml(k, v))
            elif isinstance(value, list):
                for item in value:
                    elem.append(dict_to_xml('item', item))
            else:
                elem.text = str(value)
            return elem
        
        root = dict_to_xml(root_name, d)
        return cls.from_element(root)
